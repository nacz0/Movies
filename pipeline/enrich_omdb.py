"""Enrich high-revenue reporting periods, reusing saved OMDb responses."""

from __future__ import annotations

import argparse
import json
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import duckdb

from pipeline.load_revenues import ROOT

MAX_DAILY_CAP = 900


class OmdbHttpError(ConnectionError):
    def __init__(self, status: int):
        super().__init__(f"OMDb HTTP status {status}")
        self.status = status


def load_api_key() -> str:
    key = os.environ.get("OMDB_API_KEY", "").strip()
    if not key and (ROOT / ".env").is_file():
        for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
            name, sep, value = line.partition("=")
            if sep and name.strip() == "OMDB_API_KEY":
                key = value.strip().strip('"\'')
                break
    if not key:
        raise ValueError("Set OMDB_API_KEY in the environment or project .env file")
    return key


def fetch_title(title: str, api_key: str) -> dict:
    query = urllib.parse.urlencode({"apikey": api_key, "t": title, "type": "movie"})
    request = urllib.request.Request(
        f"https://www.omdbapi.com/?{query}",
        headers={"User-Agent": "Movies-data-pipeline/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(1_000_001)
            if len(body) > 1_000_000:
                raise ValueError("OMDb response is unexpectedly large")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        # Do not print the exception: its URL contains the key.
        raise OmdbHttpError(exc.code) from None
    except (urllib.error.URLError, TimeoutError):
        raise ConnectionError("OMDb request failed") from None


def _title(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.casefold())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.findall(r"\w+", value))


def classify(payload: dict, source_title: str, first_year: int, last_year: int) -> tuple[str, str]:
    if payload.get("Response") != "True":
        error = str(payload.get("Error", "")).lower()
        if "limit" in error:
            return "quota_exhausted", "OMDb request limit"
        if "api key" in error or "invalid key" in error:
            return "invalid_key", "OMDb rejected the key"
        if "not found" in error:
            return "not_found", "Title not found"
        return "retry_later", "OMDb error"
    if payload.get("Type") != "movie" or not re.fullmatch(r"tt\d+", str(payload.get("imdbID", ""))):
        return "ambiguous", "Result is not a movie with an IMDb ID"
    if _title(str(payload.get("Title", ""))) != _title(source_title):
        return "ambiguous", "Different title"
    year = str(payload.get("Year", ""))
    if not re.fullmatch(r"\d{4}", year):
        return "ambiguous", "Missing release year"
    if not 0 <= first_year - int(year) <= 2 or last_year - first_year > 3:
        return "ambiguous", "Release year needs review"
    return "matched", "Title and year agree"


def _number(value: object, pattern: str, cast):
    match = re.fullmatch(pattern, str(value or ""))
    return cast(match.group(1)) if match else None


def _save_response(con, movie_key: int, payload: dict, status: str, reason: str,
                   fetched_at: datetime) -> None:
    con.execute("""
        INSERT INTO omdb_lookup VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (movie_key) DO UPDATE SET
            lookup_status = excluded.lookup_status,
            candidate_imdb_id = excluded.candidate_imdb_id,
            candidate_title = excluded.candidate_title,
            match_reason = excluded.match_reason,
            response_json = excluded.response_json,
            fetched_at_utc = excluded.fetched_at_utc
    """, [movie_key, status, payload.get("imdbID"), payload.get("Title"),
          reason, json.dumps(payload, ensure_ascii=False), fetched_at])
    if status == "matched":
        genre = str(payload.get("Genre", "")).split(",", 1)[0].strip()
        con.execute("""
            UPDATE dim_movie SET match_status = 'matched', imdb_id = ?,
                omdb_title = ?, release_year = ?, primary_genre = ?,
                imdb_rating = ?, runtime_minutes = ? WHERE movie_key = ?
        """, [payload["imdbID"], payload["Title"], int(payload["Year"]),
              genre if genre and genre != "N/A" else None,
              _number(payload.get("imdbRating"), r"(\d+(?:\.\d+)?)", float),
              _number(payload.get("Runtime"), r"(\d+) min", int), movie_key])
    else:
        con.execute("UPDATE dim_movie SET match_status = ? WHERE movie_key = ?",
                    [status, movie_key])


def enrich(db_path: Path, api_key: str | None, *, limit: int = 2,
           daily_cap: int = MAX_DAILY_CAP, retry_not_found: bool = False,
           fetcher=fetch_title) -> dict:
    if not 1 <= limit <= MAX_DAILY_CAP or not 1 <= daily_cap <= MAX_DAILY_CAP:
        raise ValueError("limit and daily-cap must be between 1 and 900")
    if not db_path.is_file():
        raise FileNotFoundError("Run the CSV loader first")
    counts = Counter()
    with duckdb.connect(str(db_path)) as con:
        con.execute((ROOT / "sql/omdb.sql").read_text(encoding="utf-8"))
        candidates = con.execute("""
            SELECT m.movie_key, m.source_title,
                   year(m.run_start_date), year(m.run_end_date), sum(f.revenue)
            FROM dim_movie m JOIN fact_daily_revenue f USING (movie_key)
            WHERE (? AND m.match_status = 'not_found')
               OR (NOT ? AND m.match_status IN ('pending', 'retry_later'))
            GROUP BY 1, 2, 3, 4 ORDER BY 5 DESC, 2
        """, [retry_not_found, retry_not_found]).fetchall()
        cached_by_title = {}
        for title, response, fetched_at in con.execute("""
            SELECT m.source_title, l.response_json, l.fetched_at_utc
            FROM omdb_lookup l JOIN dim_movie m USING (movie_key)
            WHERE l.response_json IS NOT NULL
              AND l.lookup_status IN ('matched', 'ambiguous', 'not_found')
            ORDER BY l.fetched_at_utc DESC
        """).fetchall():
            cached_by_title.setdefault(title, (response, fetched_at))
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(tzinfo=None)
        for movie_key, title, first_year, last_year, _ in candidates:
            cached = None if retry_not_found else cached_by_title.get(title)
            if cached:
                payload = json.loads(cached[0])
                status, reason = classify(payload, title, first_year, last_year)
                con.execute("BEGIN TRANSACTION")
                try:
                    _save_response(con, movie_key, payload, status, reason, cached[1])
                    con.execute("COMMIT")
                except Exception:
                    con.execute("ROLLBACK")
                    raise
                counts["cache_reused"] += 1
                continue

            if counts["requests"] >= limit:
                break
            used = con.execute("""
                SELECT count(*) FROM omdb_request_log WHERE requested_at_utc >= ?
            """, [cutoff]).fetchone()[0]
            if used >= daily_cap:
                counts["budget_stop"] += 1
                break
            if not api_key:
                continue
            request_id = str(uuid4())
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            con.execute("""
                INSERT INTO omdb_request_log
                (request_id, movie_key, requested_at_utc, request_status, http_status, request_kind)
                VALUES (?, ?, ?, 'reserved', NULL, 'title')
            """, [request_id, movie_key, now])
            counts["requests"] += 1
            try:
                payload = fetcher(title, api_key)
                if not isinstance(payload, dict):
                    raise ValueError("OMDb returned an invalid response")
                status, reason = classify(payload, title, first_year, last_year)
            except OmdbHttpError as exc:
                payload = None
                status = "invalid_key" if exc.status in (401, 403) else "retry_later"
                reason = "OMDb rejected the key" if status == "invalid_key" else "OMDb HTTP error"
            except (ConnectionError, ValueError, json.JSONDecodeError):
                payload, status, reason = None, "retry_later", "OMDb request failed"
            con.execute("BEGIN TRANSACTION")
            try:
                con.execute("""
                    UPDATE omdb_request_log SET request_status = ? WHERE request_id = ?
                """, [status, request_id])
                if payload is not None and status not in ("quota_exhausted", "invalid_key"):
                    _save_response(con, movie_key, payload, status, reason, now)
                    cached_by_title[title] = (json.dumps(payload, ensure_ascii=False), now)
                elif status == "retry_later":
                    con.execute("UPDATE dim_movie SET match_status = ? WHERE movie_key = ?",
                                [status, movie_key])
                con.execute("COMMIT")
            except Exception:
                con.execute("ROLLBACK")
                raise
            counts[status] += 1
            if status in ("quota_exhausted", "invalid_key"):
                break
        counts["requests_last_24h"] = con.execute("""
            SELECT count(*) FROM omdb_request_log WHERE requested_at_utc >= ?
        """, [cutoff]).fetchone()[0]
    return dict(counts)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "data/box_office.duckdb")
    parser.add_argument("--limit", type=int, default=2)
    parser.add_argument("--daily-cap", type=int, default=MAX_DAILY_CAP)
    parser.add_argument("--retry-not-found", action="store_true")
    parser.add_argument("--offline", action="store_true", help="Reuse cache without API calls")
    args = parser.parse_args()
    print(json.dumps(enrich(args.db, None if args.offline else load_api_key(),
                            limit=args.limit, daily_cap=args.daily_cap,
                            retry_not_found=args.retry_not_found), indent=2))


if __name__ == "__main__":
    main()
