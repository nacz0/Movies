"""Keep one film identity per confirmed IMDb ID across revenue periods."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]

def sync_films(con: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Refresh film identities and details inside the caller's transaction.

    `dim_film` keys never change. The run-to-film relationship is a view over
    current, confirmed assignments, so manual reversals take effect at once.
    """
    con.execute("""
        CREATE OR REPLACE TEMP TABLE current_films AS
        WITH ranked AS (
            SELECT m.imdb_id,
                   COALESCE(m.omdb_title, m.source_title) AS film_title,
                   m.release_year, m.primary_genre, m.imdb_rating,
                   m.runtime_minutes,
                   ROW_NUMBER() OVER (
                       PARTITION BY m.imdb_id
                       ORDER BY CASE WHEN m.match_status = 'matched' THEN 0 ELSE 1 END,
                                CASE WHEN m.omdb_title IS NOT NULL THEN 0 ELSE 1 END,
                                m.movie_key
                   ) AS choice
            FROM dim_movie m
            JOIN dim_release_run r ON r.movie_key = m.movie_key
            WHERE m.imdb_id IS NOT NULL
              AND m.match_status IN ('matched', 'verified_id')
        )
        SELECT imdb_id, film_title, release_year, primary_genre,
               imdb_rating, runtime_minutes
        FROM ranked WHERE choice = 1
    """)
    con.execute("""
        INSERT INTO dim_film
        SELECT nextval('film_key_seq'), c.imdb_id
        FROM current_films c
        WHERE NOT EXISTS (SELECT 1 FROM dim_film f WHERE f.imdb_id = c.imdb_id)
        ORDER BY c.imdb_id
    """)
    con.execute("DELETE FROM film_details")
    con.execute("""
        INSERT INTO film_details
        SELECT f.film_key, c.film_title, c.release_year, c.primary_genre,
               c.imdb_rating, c.runtime_minutes
        FROM current_films c JOIN dim_film f ON f.imdb_id = c.imdb_id
    """)
    confirmed_runs = con.execute("SELECT COUNT(*) FROM v_run_film_match").fetchone()[0]
    films = con.execute("SELECT COUNT(*) FROM current_films").fetchone()[0]
    if confirmed_runs < films:
        raise RuntimeError("Film identity reconciliation failed")
    return {"confirmed_films": films, "confirmed_runs": confirmed_runs}


def backfill(db_path: Path) -> dict[str, int]:
    """Upgrade an existing warehouse and derive films from saved assignments."""
    db_path = db_path.resolve()
    if not db_path.is_file():
        raise FileNotFoundError(db_path)
    with duckdb.connect(str(db_path)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            movie_columns = {row[1] for row in con.execute(
                "PRAGMA table_info('dim_movie')").fetchall()}
            required = {"movie_key", "run_start_date", "run_end_date", "display_title"}
            if not required.issubset(movie_columns):
                raise ValueError("Old title-only model detected; run pipeline.migrate_runs")
            original_facts = con.execute(
                "SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue").fetchone()
            original_matches = con.execute("""SELECT COUNT(*) FROM dim_movie m
                WHERE m.match_status IN ('matched', 'verified_id')
                  AND m.imdb_id IS NOT NULL
                  AND EXISTS (SELECT 1 FROM fact_daily_revenue f
                              WHERE f.movie_key = m.movie_key)""").fetchone()[0]
            con.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
            con.execute((ROOT / "sql" / "omdb.sql").read_text(encoding="utf-8"))
            # Older run-based databases kept the dates in dim_movie only.
            # Populate missing active runs; keep all current dates untouched.
            con.execute("""INSERT INTO dim_release_run
                SELECT m.movie_key, m.run_start_date, m.run_end_date,
                       m.display_title
                FROM dim_movie m
                WHERE EXISTS (SELECT 1 FROM fact_daily_revenue f
                              WHERE f.movie_key = m.movie_key)
                  AND NOT EXISTS (SELECT 1 FROM dim_release_run r
                                  WHERE r.movie_key = m.movie_key)""")
            orphan_facts = con.execute("""SELECT COUNT(*)
                FROM fact_daily_revenue f LEFT JOIN dim_release_run r
                  ON r.movie_key = f.movie_key
                WHERE r.movie_key IS NULL""").fetchone()[0]
            if orphan_facts:
                raise RuntimeError(f"{orphan_facts} fact rows have no release run")
            result = sync_films(con)
            result["fact_rows"] = con.execute(
                "SELECT COUNT(*) FROM fact_daily_revenue"
            ).fetchone()[0]
            if con.execute("SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue").fetchone() != original_facts:
                raise RuntimeError("Fact totals changed during film migration")
            if result["confirmed_runs"] != original_matches:
                raise RuntimeError("Confirmed movie assignments were not linked to films")
            result["omdb_requests"] = con.execute(
                "SELECT COUNT(*) FROM omdb_request_log"
            ).fetchone()[0]
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "box_office.duckdb")
    args = parser.parse_args()
    print(json.dumps(backfill(args.db), indent=2))


if __name__ == "__main__":
    main()
