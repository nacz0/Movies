"""Rebuild the title-only warehouse as title/date runs, preserving OMDb history.

This migration makes no network requests. It creates a dated backup before
replacing the database and validates revenue and request-log totals first.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb

from pipeline.enrich_omdb import apply_result, classify
from pipeline.load_revenues import ROOT, load


def migrate(csv_path: Path, db_path: Path) -> dict:
    csv_path, db_path = csv_path.resolve(), db_path.resolve()
    if not db_path.is_file():
        raise FileNotFoundError(db_path)

    with duckdb.connect(str(db_path), read_only=True) as old:
        columns = {row[1] for row in old.execute("PRAGMA table_info('dim_movie')").fetchall()}
        if "run_start_date" in columns:
            raise ValueError("Database already has the run model")
        old_totals = old.execute(
            "SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue"
        ).fetchone()
        old_movies = old.execute(
            "SELECT movie_key, source_title, imdb_id, omdb_title, release_year, "
            "primary_genre, imdb_rating, runtime_minutes, match_status FROM dim_movie"
        ).fetchall()
        old_lookups = old.execute("""
            SELECT m.source_title, l.lookup_status, l.match_reason,
                   l.response_json, l.fetched_at_utc
            FROM omdb_lookup l JOIN dim_movie m ON m.movie_key = l.movie_key
        """).fetchall()
        old_logs = old.execute("""
            SELECT l.request_id, m.source_title, l.requested_at_utc,
                   l.request_status, l.http_status
            FROM omdb_request_log l JOIN dim_movie m ON m.movie_key = l.movie_key
        """).fetchall()
        if len(old_logs) != old.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0]:
            raise RuntimeError("Cannot map all existing request log entries to titles")

    backup = db_path.with_name(
        f"{db_path.stem}_before_runs_{datetime.now():%Y%m%d_%H%M%S}_{uuid4().hex[:6]}{db_path.suffix}"
    )
    build = db_path.with_name(f"{db_path.stem}_run_build_{uuid4().hex[:8]}{db_path.suffix}")
    shutil.copy2(db_path, backup)
    summary = load(csv_path, build)
    if (summary["fact_rows"], summary["total_revenue"]) != old_totals:
        raise RuntimeError(f"CSV differs from the current database; backup: {backup}; build: {build}")

    with duckdb.connect(str(build)) as new:
        new.execute((ROOT / "sql" / "omdb.sql").read_text(encoding="utf-8"))
        new.execute("BEGIN TRANSACTION")
        try:
            # A single old title may have several new runs. Reclassify the same
            # saved response for each run, with no extra quota consumption.
            for title, previous_status, previous_reason, response_json, fetched_at in old_lookups:
                runs = new.execute("""
                    SELECT movie_key, year(run_start_date), year(run_end_date)
                    FROM dim_movie WHERE source_title = ?
                """, [title]).fetchall()
                payload = json.loads(response_json) if response_json else None
                for movie_key, first_year, last_year in runs:
                    status, reason = (
                        classify(payload, title, first_year, last_year)
                        if payload else (previous_status, previous_reason)
                    )
                    apply_result(new, movie_key, status, reason, payload, fetched_at)

            # Preserve any manually populated metadata that had no lookup row.
            cached_titles = {row[0] for row in old_lookups}
            for (_old_key, title, imdb_id, omdb_title, release_year, genre,
                 rating, runtime, status) in old_movies:
                if title in cached_titles or status == "pending":
                    continue
                target = new.execute("""
                    SELECT m.movie_key FROM dim_movie m
                    JOIN fact_daily_revenue f ON f.movie_key = m.movie_key
                    WHERE m.source_title = ?
                    GROUP BY m.movie_key ORDER BY SUM(f.revenue) DESC LIMIT 1
                """, [title]).fetchone()
                if target:
                    new.execute("""
                        UPDATE dim_movie SET imdb_id=?, omdb_title=?, release_year=?,
                            primary_genre=?, imdb_rating=?, runtime_minutes=?,
                            match_status=? WHERE movie_key=?
                    """, [imdb_id, omdb_title, release_year, genre, rating,
                           runtime, status, target[0]])

            # A historical request was issued for a title. Associate its log
            # with that title's highest-revenue run, retaining timestamp/status.
            for request_id, title, requested_at, status, http_status in old_logs:
                target = new.execute("""
                    SELECT m.movie_key FROM dim_movie m
                    JOIN fact_daily_revenue f ON f.movie_key = m.movie_key
                    WHERE m.source_title = ?
                    GROUP BY m.movie_key ORDER BY SUM(f.revenue) DESC LIMIT 1
                """, [title]).fetchone()
                if target is None:
                    raise RuntimeError(f"No new run found for logged title {title!r}")
                new.execute("""INSERT INTO omdb_request_log
                    (request_id, movie_key, requested_at_utc, request_status, http_status)
                    VALUES (?, ?, ?, ?, ?)""",
                            [request_id, target[0], requested_at, status, http_status])
            new.execute("COMMIT")
        except Exception:
            new.execute("ROLLBACK")
            raise

        result = new.execute("""
            SELECT (SELECT COUNT(*) FROM fact_daily_revenue),
                   (SELECT SUM(revenue) FROM fact_daily_revenue),
                   (SELECT COUNT(*) FROM omdb_request_log),
                   (SELECT COUNT(*) FROM omdb_lookup)
        """).fetchone()
        if result[:2] != old_totals or result[2] != len(old_logs) or result[3] < len(old_lookups):
            raise RuntimeError(f"Migration reconciliation failed: {result}; build: {build}")

    # All connections are closed before replacing the Windows database file.
    os.replace(build, db_path)
    return {**summary, "omdb_requests_preserved": len(old_logs),
            "cached_runs": result[3], "backup": str(backup)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=ROOT / "revenues_per_day.csv")
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "box_office.duckdb")
    args = parser.parse_args()
    print(json.dumps(migrate(args.csv, args.db), indent=2))


if __name__ == "__main__":
    main()
