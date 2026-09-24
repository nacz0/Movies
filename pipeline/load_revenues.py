"""Load the CSV into a small DuckDB star schema."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
RUN_GAP_DAYS = 180
CSV_COLUMNS = """{
    'id': 'VARCHAR', 'date': 'DATE', 'title': 'VARCHAR',
    'revenue': 'BIGINT', 'theaters': 'INTEGER', 'distributor': 'VARCHAR'
}"""


def load(csv_path: Path, db_path: Path) -> dict:
    csv_path, db_path = csv_path.resolve(), db_path.resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute((ROOT / "sql/schema.sql").read_text(encoding="utf-8"))
            con.execute((ROOT / "sql/omdb.sql").read_text(encoding="utf-8"))
            con.execute(f"""
                CREATE TEMP TABLE stage_revenues AS
                SELECT id AS source_id, "date" AS revenue_date,
                       title AS source_title, revenue, theaters,
                       distributor AS distributor_name
                FROM read_csv(?, header=true, columns={CSV_COLUMNS}, nullstr='')
            """, [str(csv_path)])

            rows, ids, total, missing_theaters = con.execute("""
                SELECT count(*), count(DISTINCT source_id), sum(revenue),
                       count(*) FILTER (WHERE theaters IS NULL)
                FROM stage_revenues
            """).fetchone()
            if not rows or rows != ids:
                raise ValueError("CSV is empty or has duplicate/missing IDs")
            invalid = con.execute("""
                SELECT count(*) FROM stage_revenues
                WHERE source_id IS NULL OR trim(source_id) = ''
                   OR revenue_date IS NULL
                   OR source_title IS NULL OR trim(source_title) = ''
                   OR distributor_name IS NULL OR trim(distributor_name) = ''
                   OR revenue IS NULL OR revenue < 0
                   OR (theaters IS NOT NULL AND theaters <= 0)
            """).fetchone()[0]
            if invalid:
                raise ValueError(f"CSV has {invalid} invalid rows")
            duplicates = con.execute("""
                SELECT count(*) FROM (
                    SELECT 1 FROM stage_revenues
                    GROUP BY revenue_date, source_title, distributor_name
                    HAVING count(*) > 1
                )
            """).fetchone()[0]
            if duplicates:
                raise ValueError(f"CSV has {duplicates} duplicate date/title/distributor groups")

            # A long gap gives the same source title a new reporting period.
            con.execute(f"""
                CREATE TEMP TABLE stage_dates AS
                WITH dated AS (
                    SELECT source_title, revenue_date,
                           lag(revenue_date) OVER (
                               PARTITION BY source_title ORDER BY revenue_date
                           ) AS previous_date
                    FROM (SELECT DISTINCT source_title, revenue_date FROM stage_revenues)
                )
                SELECT source_title, revenue_date,
                       sum(CASE WHEN previous_date IS NULL OR
                           date_diff('day', previous_date, revenue_date) > {RUN_GAP_DAYS}
                           THEN 1 ELSE 0 END) OVER (
                           PARTITION BY source_title ORDER BY revenue_date
                       ) AS run_number
                FROM dated
            """)
            con.execute("""
                CREATE TEMP TABLE stage_runs AS
                SELECT source_title, run_number,
                       min(revenue_date) AS run_start_date,
                       max(revenue_date) AS run_end_date
                FROM stage_dates GROUP BY 1, 2
            """)
            con.execute("""
                CREATE TEMP TABLE stage_labels AS
                SELECT source_title, run_number, run_start_date, run_end_date,
                       CASE WHEN count(*) OVER (PARTITION BY source_title) > 1
                            THEN source_title || ' (' || CAST(run_start_date AS VARCHAR) || ')'
                            ELSE source_title END AS display_title
                FROM stage_runs
            """)

            # Never silently move already-enriched source rows to another period.
            changed = con.execute("""
                SELECT old.source_title FROM fact_daily_revenue f
                JOIN dim_movie old ON old.movie_key = f.movie_key
                JOIN stage_revenues s ON s.source_id = f.source_id
                JOIN stage_dates d ON d.source_title = s.source_title
                                  AND d.revenue_date = s.revenue_date
                JOIN stage_runs r ON r.source_title = d.source_title
                                 AND r.run_number = d.run_number
                WHERE old.match_status <> 'pending'
                  AND (old.source_title <> s.source_title
                       OR old.run_start_date <> r.run_start_date)
                LIMIT 1
            """).fetchone()
            if changed:
                raise ValueError(f"Reporting period changed for enriched title: {changed[0]}")

            con.execute("DELETE FROM fact_daily_revenue")
            con.execute("DELETE FROM raw_revenues")
            con.execute("INSERT INTO raw_revenues SELECT * FROM stage_revenues")
            con.execute("""
                INSERT INTO dim_date
                SELECT CAST(strftime(revenue_date, '%Y%m%d') AS INTEGER),
                       revenue_date, year(revenue_date), quarter(revenue_date),
                       month(revenue_date), isodow(revenue_date)
                FROM (SELECT DISTINCT revenue_date FROM stage_revenues) s
                WHERE NOT EXISTS (SELECT 1 FROM dim_date d
                                  WHERE d.calendar_date = s.revenue_date)
            """)
            con.execute("""
                INSERT INTO dim_distributor
                SELECT nextval('distributor_key_seq'), s.distributor_name
                FROM (SELECT DISTINCT distributor_name FROM stage_revenues) s
                WHERE NOT EXISTS (SELECT 1 FROM dim_distributor d
                                  WHERE d.distributor_name = s.distributor_name)
                ORDER BY s.distributor_name
            """)
            con.execute("""
                INSERT INTO dim_movie (movie_key, source_title, run_start_date,
                                       run_end_date, display_title)
                SELECT nextval('movie_key_seq'), s.source_title, s.run_start_date,
                       s.run_end_date, s.display_title
                FROM stage_labels s
                WHERE NOT EXISTS (SELECT 1 FROM dim_movie m
                                  WHERE m.source_title = s.source_title
                                    AND m.run_start_date = s.run_start_date)
                ORDER BY s.source_title, s.run_start_date
            """)
            con.execute("""
                UPDATE dim_movie m SET run_end_date = s.run_end_date,
                                       display_title = s.display_title
                FROM stage_labels s
                WHERE m.source_title = s.source_title
                  AND m.run_start_date = s.run_start_date
            """)
            con.execute("""
                INSERT INTO fact_daily_revenue
                SELECT s.source_id, d.date_key, m.movie_key, x.distributor_key,
                       s.revenue, s.theaters
                FROM stage_revenues s
                JOIN stage_dates sd ON sd.source_title = s.source_title
                                   AND sd.revenue_date = s.revenue_date
                JOIN stage_runs r ON r.source_title = sd.source_title
                                 AND r.run_number = sd.run_number
                JOIN dim_movie m ON m.source_title = r.source_title
                                AND m.run_start_date = r.run_start_date
                JOIN dim_date d ON d.calendar_date = s.revenue_date
                JOIN dim_distributor x ON x.distributor_name = s.distributor_name
            """)
            loaded = con.execute("""
                SELECT count(*), sum(revenue),
                       count(*) FILTER (WHERE theaters IS NULL)
                FROM fact_daily_revenue
            """).fetchone()
            if loaded != (rows, total, missing_theaters):
                raise RuntimeError(f"Fact reconciliation failed: {loaded}")
            summary = con.execute("""
                SELECT count(DISTINCT movie_key), count(DISTINCT date_key),
                       count(DISTINCT distributor_key),
                       min(revenue_date), max(revenue_date)
                FROM fact_daily_revenue f
                JOIN raw_revenues r USING (source_id)
            """).fetchone()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    return {
        "fact_rows": rows, "periods": summary[0], "dates": summary[1],
        "distributors": summary[2], "first_date": str(summary[3]),
        "last_date": str(summary[4]), "total_revenue": total,
        "missing_theaters": missing_theaters,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=ROOT / "revenues_per_day.csv")
    parser.add_argument("--db", type=Path, default=ROOT / "data/box_office.duckdb")
    args = parser.parse_args()
    print(json.dumps(load(args.csv, args.db), indent=2))


if __name__ == "__main__":
    main()
