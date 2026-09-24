"""Find alternative OMDb films without automatically merging source runs."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import duckdb

from pipeline.enrich_omdb import (
    MAX_DAILY_CAP, OmdbHttpError, classify, fetch_query, load_api_key,
    requests_last_24h, reserve_request, set_movie_metadata,
)
from pipeline.film_identity import sync_films
from pipeline.load_revenues import ROOT


SearchFetch = Callable[[str, int | None, str], dict]
DetailFetch = Callable[[str, str], dict]
DEFAULT_DB = ROOT / "data" / "box_office.duckdb"


def fetch_search(title: str, year: int | None, api_key: str) -> dict:
    parameters: dict[str, str | int] = {"s": title, "type": "movie"}
    if year is not None:
        parameters["y"] = year
    return fetch_query(parameters, api_key)


def fetch_id(imdb_id: str, api_key: str) -> dict:
    return fetch_query({"i": imdb_id, "type": "movie"}, api_key)


def _connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    if not db_path.is_file():
        raise FileNotFoundError(f"Run the CSV loader first: {db_path}")
    con = duckdb.connect(str(db_path))
    con.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
    con.execute((ROOT / "sql" / "omdb.sql").read_text(encoding="utf-8"))
    return con


def _validate_budget(api_key: str, limit: int, daily_cap: int) -> None:
    if not api_key.strip():
        raise ValueError("OMDb API key is empty")
    if not 1 <= limit <= MAX_DAILY_CAP:
        raise ValueError(f"limit must be between 1 and {MAX_DAILY_CAP}")
    if not 1 <= daily_cap <= MAX_DAILY_CAP:
        raise ValueError(f"daily_cap must be between 1 and {MAX_DAILY_CAP}")


def set_alias(db_path: Path, source_title: str, search_title: str, reason: str) -> dict:
    source_title, search_title, reason = (value.strip() for value in
                                          (source_title, search_title, reason))
    if not all((source_title, search_title, reason)):
        raise ValueError("Source title, search title and reason are required")
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    with _connect(db_path) as con:
        if not con.execute("SELECT 1 FROM dim_movie WHERE source_title = ?",
                           [source_title]).fetchone():
            raise ValueError("Source title is not present in dim_movie")
        con.execute("""INSERT INTO title_search_alias VALUES (?, ?, ?, ?)
            ON CONFLICT (source_title, search_title) DO UPDATE SET
                reason = excluded.reason, created_at_utc = excluded.created_at_utc""",
            [source_title, search_title, reason, now])
    return {"source_title": source_title, "search_title": search_title, "reason": reason}


def remove_alias(db_path: Path, source_title: str, search_title: str) -> dict:
    with _connect(db_path) as con:
        row = con.execute("""DELETE FROM title_search_alias
            WHERE source_title = ? AND search_title = ? RETURNING source_title""",
            [source_title, search_title]).fetchone()
        if row is None:
            raise ValueError("Alias was not registered")
    return {"removed": search_title, "source_title": source_title}


def list_aliases(db_path: Path) -> list[dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute("""SELECT source_title, search_title, reason, created_at_utc
            FROM title_search_alias ORDER BY source_title, search_title""").fetchall()
    return [dict(zip(("source_title", "search_title", "reason", "created_at_utc"), row))
            for row in rows]


def list_candidates(db_path: Path, movie_key: int) -> list[dict]:
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows = con.execute("""SELECT imdb_id, candidate_title, candidate_year,
            search_title, year_hint, detail_json IS NOT NULL AS has_details
            FROM omdb_candidate WHERE movie_key = ? ORDER BY candidate_year, candidate_title""",
            [movie_key]).fetchall()
    return [dict(zip(("imdb_id", "candidate_title", "candidate_year",
                      "search_title", "year_hint", "has_details"), row)) for row in rows]


def budget(db_path: Path, daily_cap: int = MAX_DAILY_CAP) -> dict[str, int]:
    if not 1 <= daily_cap <= MAX_DAILY_CAP:
        raise ValueError(f"daily_cap must be between 1 and {MAX_DAILY_CAP}")
    with duckdb.connect(str(db_path), read_only=True) as con:
        used = requests_last_24h(con)
    return {"requests_last_24h": used, "daily_cap": daily_cap,
            "remaining": max(0, daily_cap - used)}


def _save_search(con: duckdb.DuckDBPyConnection, movie_key: int, title: str,
                 year_hint: int, payload: dict) -> int:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute("""INSERT INTO omdb_search_cache VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (movie_key, search_title, year_hint) DO UPDATE SET
            response_json = excluded.response_json,
            fetched_at_utc = excluded.fetched_at_utc""",
        [movie_key, title, year_hint, json.dumps(payload, ensure_ascii=False), now])
    count = 0
    for item in payload.get("Search", []):
        if not isinstance(item, dict) or item.get("Type") != "movie":
            continue
        imdb_id = str(item.get("imdbID", ""))
        candidate_title = str(item.get("Title", "")).strip()
        if not re.fullmatch(r"tt\d+", imdb_id) or not candidate_title:
            continue
        con.execute("""INSERT INTO omdb_candidate
            (movie_key, imdb_id, candidate_title, candidate_year, search_title,
             year_hint, detail_json, discovered_at_utc)
            VALUES (?, ?, ?, ?, ?, ?, NULL, ?)
            ON CONFLICT (movie_key, imdb_id) DO UPDATE SET
                candidate_title = excluded.candidate_title,
                candidate_year = excluded.candidate_year,
                search_title = excluded.search_title,
                year_hint = excluded.year_hint""",
            [movie_key, imdb_id, candidate_title, str(item.get("Year", "")),
             title, year_hint, now])
        count += 1
    return count


def search_candidates(
    db_path: Path, api_key: str, *, limit: int = 1,
    daily_cap: int = MAX_DAILY_CAP, movie_key: int | None = None,
    search_title: str | None = None, year: int | None = None,
    broad: bool = False, refresh: bool = False,
    fetcher: SearchFetch = fetch_search,
) -> dict[str, int]:
    _validate_budget(api_key, limit, daily_cap)
    if broad and year is not None:
        raise ValueError("Choose either a year hint or --broad")
    if year is not None and not 1870 <= year <= 2100:
        raise ValueError("year must be between 1870 and 2100")
    if (search_title is not None or year is not None) and movie_key is None:
        raise ValueError("--search-title and --year require --movie-key")
    if refresh and movie_key is None:
        raise ValueError("--refresh requires --movie-key")
    counts: Counter[str] = Counter()
    with _connect(db_path) as con:
        runs = con.execute("""SELECT m.movie_key, m.source_title,
                year(r.run_start_date) AS first_year
            FROM dim_movie m JOIN dim_release_run r USING (movie_key)
            JOIN fact_daily_revenue f USING (movie_key)
            WHERE m.match_status IN ('ambiguous', 'not_found', 'rejected')
              AND (? IS NULL OR m.movie_key = ?)
            GROUP BY m.movie_key, m.source_title, r.run_start_date
            ORDER BY SUM(f.revenue) DESC, m.movie_key""",
            [movie_key, movie_key]).fetchall()
        if movie_key is not None and not runs:
            raise ValueError("Run is missing or not eligible for candidate search")
        new_requests = 0
        for key, source_title, first_year in runs:
            if new_requests >= limit:
                break
            query_title = (search_title or source_title).strip()
            if not query_title:
                raise ValueError("Search title is empty")
            if query_title != source_title and not con.execute("""
                SELECT 1 FROM title_search_alias WHERE source_title = ? AND search_title = ?
            """, [source_title, query_title]).fetchone():
                raise ValueError("Register this alias before using it in a search")
            hint = 0 if broad else (year or first_year)
            cached = con.execute("""SELECT response_json FROM omdb_search_cache
                WHERE movie_key = ? AND search_title = ? AND year_hint = ?""",
                [key, query_title, hint]).fetchone()
            if cached and not refresh:
                counts["cache_reused"] += 1
                continue
            request_id = reserve_request(con, key, daily_cap, "search")
            if request_id is None:
                counts["budget_stop"] += 1
                break
            new_requests += 1
            try:
                payload = fetcher(query_title, hint or None, api_key)
                if not isinstance(payload, dict):
                    raise ValueError("OMDb returned a non-object response")
                status, _ = classify(payload, source_title, first_year, first_year)
                # A search response has a Search array, not a single movie detail.
                if payload.get("Response") == "True":
                    if not isinstance(payload.get("Search"), list):
                        raise ValueError("OMDb search response has no result list")
                    status = "searched"
                con.execute("BEGIN TRANSACTION")
                try:
                    con.execute("""UPDATE omdb_request_log SET request_status = ?
                        WHERE request_id = ?""", [status, request_id])
                    if status in ("searched", "not_found"):
                        counts["candidates_saved"] += _save_search(
                            con, key, query_title, hint, payload)
                    con.execute("COMMIT")
                except Exception:
                    con.execute("ROLLBACK")
                    raise
            except OmdbHttpError as exc:
                status = "invalid_key" if exc.status in (401, 403) else "retry_later"
                con.execute("""UPDATE omdb_request_log SET request_status = ?,
                    http_status = ? WHERE request_id = ?""", [status, exc.status, request_id])
            except (ConnectionError, ValueError, json.JSONDecodeError):
                status = "retry_later"
                con.execute("""UPDATE omdb_request_log SET request_status = ?
                    WHERE request_id = ?""", [status, request_id])
            counts[status] += 1
            if status in ("invalid_key", "quota_exhausted"):
                break
        counts["requests_last_24h"] = requests_last_24h(con)
    return dict(counts)


def hydrate_verified(
    db_path: Path, api_key: str, *, limit: int = 1,
    daily_cap: int = MAX_DAILY_CAP, movie_key: int | None = None,
    fetcher: DetailFetch = fetch_id,
) -> dict[str, int]:
    """Fetch details only for IMDb IDs explicitly confirmed by a reviewer."""
    _validate_budget(api_key, limit, daily_cap)
    counts: Counter[str] = Counter()
    with _connect(db_path) as con:
        runs = con.execute("""SELECT m.movie_key, m.imdb_id
            FROM dim_movie m JOIN movie_match_decision d USING (movie_key)
            WHERE m.match_status = 'verified_id'
              AND d.decision = 'verify_id'
              AND m.imdb_id = d.imdb_id
              AND (? IS NULL OR m.movie_key = ?)
            ORDER BY m.movie_key LIMIT ?""", [movie_key, movie_key, limit]).fetchall()
        if movie_key is not None and not runs:
            raise ValueError("Run is missing or has no verified ID awaiting details")
        for key, imdb_id in runs:
            cached = con.execute("""SELECT detail_json FROM omdb_candidate
                WHERE movie_key = ? AND imdb_id = ?""", [key, imdb_id]).fetchone()
            if cached and cached[0]:
                payload = json.loads(cached[0])
                set_movie_metadata(con, key, payload)
                sync_films(con)
                counts["cache_reused"] += 1
                continue
            request_id = reserve_request(con, key, daily_cap, "detail")
            if request_id is None:
                counts["budget_stop"] += 1
                break
            try:
                payload = fetcher(imdb_id, api_key)
                if not isinstance(payload, dict):
                    raise ValueError("OMDb returned a non-object response")
                if payload.get("Response") == "True":
                    valid = (payload.get("Type") == "movie" and
                             payload.get("imdbID") == imdb_id and
                             bool(str(payload.get("Title", "")).strip()) and
                             re.fullmatch(r"\d{4}", str(payload.get("Year", ""))))
                    status = "hydrated" if valid else "ambiguous"
                else:
                    status, _ = classify(payload, "", 0, 0)
                con.execute("BEGIN TRANSACTION")
                try:
                    con.execute("""UPDATE omdb_request_log SET request_status = ?
                        WHERE request_id = ?""", [status, request_id])
                    if status == "hydrated":
                        now = datetime.now(timezone.utc).replace(tzinfo=None)
                        con.execute("""INSERT INTO omdb_candidate
                            (movie_key, imdb_id, candidate_title, candidate_year,
                             detail_json, discovered_at_utc)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT (movie_key, imdb_id) DO UPDATE SET
                                candidate_title = excluded.candidate_title,
                                candidate_year = excluded.candidate_year,
                                detail_json = excluded.detail_json""",
                            [key, imdb_id, payload["Title"], payload["Year"],
                             json.dumps(payload, ensure_ascii=False), now])
                        set_movie_metadata(con, key, payload)
                        sync_films(con)
                    con.execute("COMMIT")
                except Exception:
                    con.execute("ROLLBACK")
                    raise
            except OmdbHttpError as exc:
                status = "invalid_key" if exc.status in (401, 403) else "retry_later"
                con.execute("""UPDATE omdb_request_log SET request_status = ?,
                    http_status = ? WHERE request_id = ?""", [status, exc.status, request_id])
            except (ConnectionError, ValueError, json.JSONDecodeError):
                status = "retry_later"
                con.execute("""UPDATE omdb_request_log SET request_status = ?
                    WHERE request_id = ?""", [status, request_id])
            counts[status] += 1
            if status in ("invalid_key", "quota_exhausted"):
                break
        counts["requests_last_24h"] = requests_last_24h(con)
    return dict(counts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    commands = parser.add_subparsers(dest="command", required=True)
    aliases = commands.add_parser("aliases")
    alias_action = aliases.add_subparsers(dest="alias_command", required=True)
    add = alias_action.add_parser("set")
    add.add_argument("--source-title", required=True)
    add.add_argument("--search-title", required=True)
    add.add_argument("--reason", required=True)
    remove = alias_action.add_parser("remove")
    remove.add_argument("--source-title", required=True)
    remove.add_argument("--search-title", required=True)
    alias_action.add_parser("list")
    search = commands.add_parser("search")
    search.add_argument("--limit", type=int, default=1)
    search.add_argument("--daily-cap", type=int, default=MAX_DAILY_CAP)
    search.add_argument("--movie-key", type=int)
    search.add_argument("--search-title")
    search.add_argument("--year", type=int)
    search.add_argument("--broad", action="store_true")
    search.add_argument("--refresh", action="store_true",
                        help="Requery one selected run even if its search is cached")
    detail = commands.add_parser("hydrate")
    detail.add_argument("--limit", type=int, default=1)
    detail.add_argument("--daily-cap", type=int, default=MAX_DAILY_CAP)
    detail.add_argument("--movie-key", type=int)
    listing = commands.add_parser("list")
    listing.add_argument("--movie-key", type=int, required=True)
    budget_command = commands.add_parser("budget")
    budget_command.add_argument("--daily-cap", type=int, default=MAX_DAILY_CAP)
    args = parser.parse_args()
    if args.command == "aliases":
        if args.alias_command == "set":
            result = set_alias(args.db, args.source_title, args.search_title, args.reason)
        elif args.alias_command == "remove":
            result = remove_alias(args.db, args.source_title, args.search_title)
        else:
            result = list_aliases(args.db)
    elif args.command == "search":
        result = search_candidates(args.db, load_api_key(), limit=args.limit,
                                   daily_cap=args.daily_cap, movie_key=args.movie_key,
                                   search_title=args.search_title, year=args.year,
                                   broad=args.broad, refresh=args.refresh)
    elif args.command == "hydrate":
        result = hydrate_verified(args.db, load_api_key(), limit=args.limit,
                                  daily_cap=args.daily_cap, movie_key=args.movie_key)
    elif args.command == "budget":
        result = budget(args.db, args.daily_cap)
    else:
        result = list_candidates(args.db, args.movie_key)
    print(json.dumps(result, ensure_ascii=False, default=str, indent=2))


if __name__ == "__main__":
    main()
