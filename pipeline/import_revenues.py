"""Validate and import CSV into the initialized working database; dbt owns transforms."""

from __future__ import annotations

import hashlib
from pathlib import Path

import duckdb

CSV_COLUMNS = """{
    'id': 'VARCHAR', 'date': 'DATE', 'title': 'VARCHAR',
    'revenue': 'BIGINT', 'theaters': 'INTEGER', 'distributor': 'VARCHAR'
}"""


def import_snapshot(csv_path: Path, db_path: Path) -> dict:
    csv_path, db_path = csv_path.resolve(), db_path.resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute("BEGIN TRANSACTION")
        try:
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

            con.execute("DELETE FROM raw_revenues")
            con.execute("INSERT INTO raw_revenues SELECT * FROM stage_revenues")
            with csv_path.open('rb') as handle:
                fingerprint = hashlib.file_digest(handle, 'sha256').hexdigest()
            con.execute("""INSERT INTO import_history
                VALUES (timezone('UTC',current_timestamp), ?, ?, ?, ?)""",
                [fingerprint, rows, total, missing_theaters])
            con.execute("COMMIT")
        except BaseException:
            con.execute("ROLLBACK")
            raise
    return {"rows": rows, "total_revenue": total, "missing_theaters": missing_theaters}
