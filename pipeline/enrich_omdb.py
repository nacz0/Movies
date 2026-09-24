"""Enrich a few uncached movie titles through OMDb with a hard request budget."""

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
from typing import Callable
from uuid import uuid4

import duckdb

from pipeline.film_identity import sync_films
from pipeline.load_revenues import ROOT


API_URL = "https://www.omdbapi.com/"
MAX_DAILY_CAP = 900  # Leave room below OMDb's published 1,000/day free limit.
Fetch = Callable[[str, str], dict]


class OmdbHttpError(Exception):
    def __init__(self, status: int):
        super().__init__(f"OMDb HTTP status {status}")
        self.status = status


def load_api_key(env_path: Path = ROOT / ".env") -> str:
    key = os.environ.get("OMDB_API_KEY", "").strip()
    if key:
        return key
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            name, separator, value = line.partition("=")
            if separator and name.strip() == "OMDB_API_KEY":
                key = value.strip().strip('"\'')
                if key:
                    return key
    raise ValueError("Set OMDB_API_KEY in the environment or project .env file")


def fetch_title(title: str, api_key: str) -> dict:
    return fetch_query({"t": title, "type": "movie"}, api_key)


def fetch_query(parameters: dict[str, str | int], api_key: str) -> dict:
    query = urllib.parse.urlencode({"apikey": api_key, **parameters})
    request = urllib.request.Request(
        f"{API_URL}?{query}", headers={"User-Agent": "Movies-data-pipeline/1.0"}
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(1_000_001)
            if len(body) > 1_000_000:
                raise ValueError("OMDb response is unexpectedly large")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        # Do not include exc or the request URL in messages: both contain the key.
        raise OmdbHttpError(exc.code) from None
    except (urllib.error.URLError, TimeoutError):
        raise ConnectionError("OMDb request failed") from None


def reserve_request(
    con: duckdb.DuckDBPyConnection, movie_key: int, daily_cap: int, kind: str
) -> str | None:
    """Commit one conservative reservation against the shared rolling budget."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(tzinfo=None)
    used = con.execute(
        "SELECT COUNT(*) FROM omdb_request_log WHERE requested_at_utc >= ?", [cutoff]
    ).fetchone()[0]
    if used >= daily_cap:
        return None
    request_id = str(uuid4())
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute(
        """INSERT INTO omdb_request_log
           (request_id, movie_key, requested_at_utc, request_status, http_status, request_kind)
           VALUES (?, ?, ?, 'reserved', NULL, ?)""",
        [request_id, movie_key, now, kind],
    )
    return request_id


def requests_last_24h(con: duckdb.DuckDBPyConnection) -> int:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(tzinfo=None)
    return con.execute(
        "SELECT COUNT(*) FROM omdb_request_log WHERE requested_at_utc >= ?", [cutoff]
    ).fetchone()[0]


def normalized_title(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.casefold())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.findall(r"\w+", value))


def classify(
    payload: dict, source_title: str, first_year: int, last_year: int
) -> tuple[str, str]:
    if payload.get("Response") != "True":
        error = str(payload.get("Error", "")).lower()
        if "limit" in error:
            return "quota_exhausted", "OMDb reported a request limit"
        if "api key" in error or "invalid key" in error:
            return "invalid_key", "OMDb rejected the API key"
        if "not found" in error:
            return "not_found", "OMDb found no title"
        return "retry_later", "OMDb returned an error"

    if payload.get("Type") != "movie":
        return "ambiguous", "OMDb result is not a movie"
    if not re.fullmatch(r"tt\d+", str(payload.get("imdbID", ""))):
        return "ambiguous", "OMDb result has no valid IMDb ID"
    if normalized_title(str(payload.get("Title", ""))) != normalized_title(source_title):
        return "ambiguous", "Title differs from the source title"
    year_text = str(payload.get("Year", ""))
    if not re.fullmatch(r"\d{4}", year_text):
        return "ambiguous", "OMDb release year is missing or unclear"
    if int(year_text) > first_year:
        return "ambiguous", "OMDb release year is after the first revenue date"
    if first_year - int(year_text) > 2:
        return "ambiguous", "OMDb release year is much earlier than first revenue date"
    if last_year - first_year > 3:
        return "ambiguous", "Source title spans several years and may include rereleases"
    return "matched", "Exact normalized title and plausible release year"


def _nullable_number(value: object, pattern: str, cast: Callable) -> object | None:
    match = re.fullmatch(pattern, str(value or ""))
    return cast(match.group(1)) if match else None


def set_movie_metadata(
    con: duckdb.DuckDBPyConnection, movie_key: int, payload: dict
) -> None:
    """Populate the movie dimension from a validated OMDb movie payload."""
    genre = str(payload.get("Genre", "")).split(",", 1)[0].strip()
    con.execute(
        """
        UPDATE dim_movie
        SET imdb_id = ?, omdb_title = ?, release_year = ?,
            primary_genre = ?, imdb_rating = ?, runtime_minutes = ?,
            match_status = 'matched'
        WHERE movie_key = ?
        """,
        [
            payload["imdbID"], payload["Title"], int(payload["Year"]),
            genre if genre and genre != "N/A" else None,
            _nullable_number(payload.get("imdbRating"), r"(\d+(?:\.\d+)?)", float),
            _nullable_number(payload.get("Runtime"), r"(\d+) min", int),
            movie_key,
        ],
    )


def _record_result(
    con: duckdb.DuckDBPyConnection,
    request_id: str,
    movie_key: int,
    status: str,
    reason: str,
    payload: dict | None,
    http_status: int | None = None,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            "UPDATE omdb_request_log SET request_status = ?, http_status = ? WHERE request_id = ?",
            [status, http_status, request_id],
        )
        apply_result(con, movie_key, status, reason, payload, now)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def apply_result(
    con: duckdb.DuckDBPyConnection,
    movie_key: int,
    status: str,
    reason: str,
    payload: dict | None,
    fetched_at_utc: datetime,
) -> None:
    """Apply a network or cached response inside the caller's transaction."""
    con.execute(
            """
            INSERT INTO omdb_lookup VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (movie_key) DO UPDATE SET
                lookup_status = excluded.lookup_status,
                candidate_imdb_id = excluded.candidate_imdb_id,
                candidate_title = excluded.candidate_title,
                match_reason = excluded.match_reason,
                response_json = excluded.response_json,
                fetched_at_utc = excluded.fetched_at_utc
            """,
            [
                movie_key,
                status,
                payload.get("imdbID") if payload else None,
                payload.get("Title") if payload else None,
                reason,
                json.dumps(payload, ensure_ascii=False) if payload else None,
                fetched_at_utc,
            ],
        )
    if status == "matched" and payload is not None:
        set_movie_metadata(con, movie_key, payload)
    elif status not in ("quota_exhausted", "invalid_key"):
            con.execute(
                "UPDATE dim_movie SET match_status = ? WHERE movie_key = ?",
                [status, movie_key],
            )
    sync_films(con)


def enrich(
    db_path: Path,
    api_key: str,
    *,
    limit: int = 2,
    daily_cap: int = MAX_DAILY_CAP,
    retry_errors: bool = False,
    retry_not_found: bool = False,
    movie_key: int | None = None,
    fetcher: Fetch = fetch_title,
) -> dict[str, int | str]:
    if limit < 1 or limit > MAX_DAILY_CAP:
        raise ValueError(f"limit must be between 1 and {MAX_DAILY_CAP}")
    if daily_cap < 1 or daily_cap > MAX_DAILY_CAP:
        raise ValueError(f"daily_cap must be between 1 and {MAX_DAILY_CAP}")
    if not api_key.strip():
        raise ValueError("OMDb API key is empty")
    if not db_path.is_file():
        raise FileNotFoundError(f"Run the CSV loader first: {db_path}")

    counts: Counter[str] = Counter()
    con = duckdb.connect(str(db_path))
    try:
        con.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
        con.execute((ROOT / "sql" / "omdb.sql").read_text(encoding="utf-8"))
        candidates = con.execute(
            """
            SELECT m.movie_key, m.source_title, year(r.run_start_date) AS first_year,
                   year(r.run_end_date) AS last_year,
                   SUM(f.revenue) AS total_revenue
            FROM dim_movie m
            JOIN dim_release_run r ON r.movie_key = m.movie_key
            JOIN fact_daily_revenue f ON f.movie_key = m.movie_key
            WHERE (m.match_status = 'pending'
               OR (? AND m.match_status = 'retry_later')
               OR (? AND m.match_status = 'not_found'))
              AND (? IS NULL OR m.movie_key = ?)
            GROUP BY m.movie_key, m.source_title, r.run_start_date, r.run_end_date
            ORDER BY total_revenue DESC, m.source_title
            LIMIT ?
            """,
            [retry_errors, retry_not_found, movie_key, movie_key, limit],
        ).fetchall()

        for movie_key, title, first_year, last_year, _ in candidates:
            cached = con.execute("""
                SELECT l.response_json, l.fetched_at_utc
                FROM omdb_lookup l JOIN dim_movie m ON m.movie_key = l.movie_key
                WHERE m.source_title = ? AND l.response_json IS NOT NULL
                  AND l.lookup_status IN ('matched', 'ambiguous')
                  AND json_extract_string(l.response_json, '$.Response') = 'True'
                ORDER BY l.fetched_at_utc DESC LIMIT 1
            """, [title]).fetchone()
            if cached:
                payload = json.loads(cached[0])
                status, reason = classify(payload, title, first_year, last_year)
                con.execute("BEGIN TRANSACTION")
                try:
                    apply_result(con, movie_key, status, reason, payload, cached[1])
                    con.execute("COMMIT")
                except Exception:
                    con.execute("ROLLBACK")
                    raise
                counts["cache_reused"] += 1
                continue
            request_id = reserve_request(con, movie_key, daily_cap, "title")
            if request_id is None:
                counts["budget_stop"] += 1
                break
            try:
                payload = fetcher(title, api_key)
                if not isinstance(payload, dict):
                    raise ValueError("OMDb returned a non-object response")
                status, reason = classify(payload, title, first_year, last_year)
                _record_result(con, request_id, movie_key, status, reason, payload)
            except OmdbHttpError as exc:
                status = "invalid_key" if exc.status in (401, 403) else "retry_later"
                reason = "OMDb rejected the API key" if status == "invalid_key" else "OMDb HTTP error"
                _record_result(con, request_id, movie_key, status, reason, None, exc.status)
            except (ConnectionError, ValueError, json.JSONDecodeError):
                status = "retry_later"
                _record_result(con, request_id, movie_key, status, "OMDb request failed", None)

            counts[status] += 1
            if status in ("quota_exhausted", "invalid_key"):
                break

        counts["requests_last_24h"] = requests_last_24h(con)
        counts["remaining_pending"] = con.execute(
            """SELECT COUNT(*) FROM dim_movie m
                WHERE m.match_status = 'pending' AND EXISTS
                (SELECT 1 FROM fact_daily_revenue f WHERE f.movie_key = m.movie_key)"""
        ).fetchone()[0]
        return dict(counts)
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "box_office.duckdb")
    parser.add_argument("--limit", type=int, default=2, help="Maximum requests this run (default: 2)")
    parser.add_argument("--daily-cap", type=int, default=MAX_DAILY_CAP)
    parser.add_argument("--retry-errors", action="store_true", help="Retry previous network errors")
    parser.add_argument("--retry-not-found", action="store_true",
                        help="Refresh previous title-not-found responses")
    parser.add_argument("--movie-key", type=int,
                        help="Only process one selected revenue period")
    args = parser.parse_args()
    print(
        json.dumps(
            enrich(
                args.db,
                load_api_key(),
                limit=args.limit,
                daily_cap=args.daily_cap,
                retry_errors=args.retry_errors,
                retry_not_found=args.retry_not_found,
                movie_key=args.movie_key,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
