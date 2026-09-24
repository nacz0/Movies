"""Validate a daily-revenue CSV and load a small dimensional model into DuckDB."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4

import duckdb

from pipeline.film_identity import sync_films


ROOT = Path(__file__).resolve().parents[1]
RUN_GAP_DAYS = 180
CSV_COLUMNS = """{
    'id': 'VARCHAR',
    'date': 'DATE',
    'title': 'VARCHAR',
    'revenue': 'BIGINT',
    'theaters': 'INTEGER',
    'distributor': 'VARCHAR'
}"""


def _reconcile_runs(
    con: duckdb.DuckDBPyConnection,
    allowed_boundary_title: str | None = None,
) -> tuple[dict[str, int], list[tuple], list[tuple], set[int]]:
    """Preserve keys for one-to-one run extensions; block unsafe remapping."""
    con.execute("""
        CREATE TEMP TABLE stage_overlap AS
        SELECT f.movie_key AS old_movie_key, sr.source_title,
               sr.run_start_date, COUNT(*) AS shared_source_rows
        FROM fact_daily_revenue f
        JOIN dim_movie m ON m.movie_key = f.movie_key
        JOIN stage_revenues s ON s.source_id = f.source_id
                             AND s.source_title = m.source_title
        JOIN stage_dates sd ON sd.source_title = s.source_title
                           AND sd.revenue_date = s.revenue_date
        JOIN stage_runs sr ON sr.source_title = sd.source_title
                          AND sr.run_number = sd.run_number
        GROUP BY f.movie_key, sr.source_title, sr.run_start_date
    """)
    overlaps = con.execute("""
        SELECT old_movie_key, source_title, run_start_date
        FROM stage_overlap
    """).fetchall()
    old_to_new: dict[int, set[tuple[str, object]]] = defaultdict(set)
    new_to_old: dict[tuple[str, object], set[int]] = defaultdict(set)
    for old_key, title, start in overlaps:
        new_run = (title, start)
        old_to_new[old_key].add(new_run)
        new_to_old[new_run].add(old_key)

    existing = con.execute("""
        SELECT m.movie_key, m.source_title,
               COALESCE(r.run_start_date, m.run_start_date),
               COALESCE(r.run_end_date, m.run_end_date), m.match_status,
               m.run_start_date AS anchor_start_date
        FROM dim_movie m LEFT JOIN dim_release_run r ON r.movie_key = m.movie_key
        ORDER BY CASE WHEN r.movie_key IS NULL THEN 1 ELSE 0 END
    """).fetchall()
    by_key = {row[0]: row for row in existing}
    by_title_start = {}
    by_anchor = {}
    for row in existing:
        by_title_start.setdefault((row[1], row[2]), row)
        by_anchor[(row[1], row[5])] = row
    active_old = {row[0] for row in con.execute(
        "SELECT DISTINCT movie_key FROM fact_daily_revenue"
    ).fetchall()}
    new_runs = con.execute("""
        SELECT source_title, run_start_date, run_end_date FROM stage_runs
    """).fetchall()

    structural_old = {key for key, runs in old_to_new.items() if len(runs) > 1}
    for old_keys in new_to_old.values():
        if len(old_keys) > 1:
            structural_old.update(old_keys)
    # An exact title/start key is reused by the INSERT below. If its source
    # records no longer map to that run, its existing review cannot be reused.
    for title, start, _ in new_runs:
        exact = by_title_start.get((title, start))
        if exact and exact[0] in active_old and exact[0] not in new_to_old[(title, start)]:
            structural_old.add(exact[0])
        if exact and exact[0] not in active_old and exact[4] != "pending":
            structural_old.add(exact[0])
        anchor = by_anchor.get((title, start))
        if anchor and anchor[0] in active_old and anchor[0] not in new_to_old[(title, start)]:
            structural_old.add(anchor[0])
        if anchor and anchor[0] not in active_old and anchor[4] != "pending":
            structural_old.add(anchor[0])

    if structural_old:
        rows = [by_key[key] for key in structural_old]
        reviewed = [row for row in rows if row[4] != "pending"]
        manually_decided = {row[0] for row in con.execute("""
            SELECT movie_key FROM movie_match_decision WHERE movie_key IN
            (SELECT unnest(?::BIGINT[]))""", [list(structural_old)]).fetchall()}
        unsafe = [row for row in reviewed if row[1] != allowed_boundary_title
                  or row[0] in manually_decided]
        if unsafe:
            examples = ", ".join(f"{row[1]} [{row[0]}]" for row in unsafe[:5])
            raise ValueError(
                "Run split/merge or source-ID replacement affects reviewed movies: "
                f"{examples}. Undo manual match decisions before changing this "
                "boundary; existing assignments were preserved."
            )

    assigned: dict[tuple[str, object], int] = {}
    used_keys = set()
    # A historical anchor has priority when a later split exposes its date
    # again. This also respects dim_movie's unique (title, anchor) constraint.
    for title, start, _ in new_runs:
        anchor = by_anchor.get((title, start))
        if anchor:
            assigned[(title, start)] = anchor[0]
            used_keys.add(anchor[0])
    assignments = []
    for title, start, end in new_runs:
        key = assigned.get((title, start))
        exact = by_title_start.get((title, start))
        if key is None and exact and exact[0] not in used_keys:
            key = exact[0]
        if key is None and len(new_to_old[(title, start)]) == 1:
            old_key = next(iter(new_to_old[(title, start)]))
            if (len(old_to_new[old_key]) == 1 and old_key not in structural_old
                    and old_key not in used_keys):
                key = old_key
        if key in used_keys and assigned.get((title, start)) != key:
            raise ValueError(f"Several new periods map to movie_key {key}; review snapshot")
        if key is not None:
            used_keys.add(key)
        assignments.append((title, start, end, key))

    moves = []
    for title, start, end, key in assignments:
        if key is None or key not in active_old or key in structural_old:
            continue
        new_run = (title, start)
        if len(new_to_old[new_run]) != 1 or len(old_to_new[key]) != 1:
            continue
        old = by_key[key]
        if (old[2], old[3]) != (start, end):
            moves.append((key, old[2], old[3], start, end, old[4]))

    return ({"boundary_changes": len(moves),
             "pending_structure_changes": len(structural_old)}, moves,
            assignments, structural_old)


def load(csv_path: Path, db_path: Path, *,
         boundary_change: tuple[str, date, str | None, str] | None = None
         ) -> dict[str, int | str]:
    csv_path = csv_path.resolve()
    db_path = db_path.resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    try:
        old_model = con.execute("""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'dim_movie'""").fetchone()[0]
        if old_model and not con.execute("""SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'dim_movie' AND column_name = 'run_start_date'""").fetchone()[0]:
            raise RuntimeError("Old title-only model detected; run python -m pipeline.migrate_runs")
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute((ROOT / "sql" / "schema.sql").read_text(encoding="utf-8"))
            con.execute((ROOT / "sql" / "omdb.sql").read_text(encoding="utf-8"))
            con.execute(
                f"""
                CREATE TEMP TABLE stage_revenues AS
                SELECT
                    id AS source_id,
                    "date" AS revenue_date,
                    title AS source_title,
                    revenue,
                    theaters,
                    distributor AS distributor_name
                FROM read_csv(?, header=true, columns={CSV_COLUMNS}, nullstr='')
                """,
                [str(csv_path)],
            )

            source = con.execute(
                """
                SELECT COUNT(*) AS row_count,
                       COUNT(DISTINCT source_id) AS unique_ids,
                       SUM(revenue) AS total_revenue,
                       COUNT(*) FILTER (WHERE theaters IS NULL) AS missing_theaters
                FROM stage_revenues
                """
            ).fetchone()
            assert source is not None
            row_count, unique_ids, total_revenue, missing_theaters = source
            if row_count == 0:
                raise ValueError("CSV contains no data rows")
            if unique_ids != row_count:
                raise ValueError("CSV contains duplicate or missing source IDs")

            invalid = con.execute(
                """
                SELECT COUNT(*) FROM stage_revenues
                WHERE source_id IS NULL OR trim(source_id) = ''
                   OR revenue_date IS NULL
                   OR source_title IS NULL OR trim(source_title) = ''
                   OR distributor_name IS NULL OR trim(distributor_name) = ''
                   OR revenue IS NULL OR revenue < 0
                   OR (theaters IS NOT NULL AND theaters <= 0)
                """
            ).fetchone()[0]
            if invalid:
                raise ValueError(f"CSV contains {invalid} rows with invalid required values")

            duplicate_grain = con.execute(
                """
                SELECT COUNT(*) FROM (
                    SELECT 1 FROM stage_revenues
                    GROUP BY revenue_date, source_title, distributor_name
                    HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
            if duplicate_grain:
                raise ValueError(
                    f"CSV contains {duplicate_grain} duplicate date/title/distributor groups"
                )

            if boundary_change is not None:
                title, boundary_date, action, reason = boundary_change
                if not title.strip() or not reason.strip():
                    raise ValueError("Boundary title and review reason are required")
                if action not in ("split", "join", None):
                    raise ValueError("Boundary action must be split, join or remove")
                preceding = con.execute("""SELECT max(revenue_date)
                    FROM stage_revenues WHERE source_title = ? AND revenue_date < ?""",
                    [title, boundary_date]).fetchone()[0]
                present = con.execute("""SELECT 1 FROM stage_revenues
                    WHERE source_title = ? AND revenue_date = ? LIMIT 1""",
                    [title, boundary_date]).fetchone()
                if not present or preceding is None:
                    raise ValueError("Boundary date must be a non-first revenue date for this title")
                natural_split = (boundary_date - preceding).days > RUN_GAP_DAYS
                if action == "split" and natural_split:
                    raise ValueError("This date is already an automatic split")
                if action == "join" and not natural_split:
                    raise ValueError("This date is already joined by the automatic rule")
                old_rule = con.execute("""SELECT action FROM title_boundary_rule
                    WHERE source_title = ? AND boundary_date = ?""",
                    [title, boundary_date]).fetchone()
                if action is None:
                    if old_rule is None:
                        raise ValueError("No manual boundary rule exists at this date")
                    con.execute("""DELETE FROM title_boundary_rule
                        WHERE source_title = ? AND boundary_date = ?""",
                        [title, boundary_date])
                else:
                    now = datetime.now(timezone.utc).replace(tzinfo=None)
                    con.execute("""INSERT INTO title_boundary_rule VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT (source_title, boundary_date) DO UPDATE SET
                            action = excluded.action, reason = excluded.reason,
                            reviewed_at_utc = excluded.reviewed_at_utc""",
                        [title, boundary_date, action, reason, now])
                con.execute("""INSERT INTO title_boundary_rule_event
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    [str(uuid4()), title, boundary_date,
                     old_rule[0] if old_rule else None, action, reason,
                     datetime.now(timezone.utc).replace(tzinfo=None)])

            obsolete_rule = con.execute("""SELECT source_title, boundary_date
                FROM title_boundary_rule rule WHERE NOT EXISTS (
                    SELECT 1 FROM stage_revenues s
                    WHERE s.source_title = rule.source_title
                      AND s.revenue_date = rule.boundary_date)
                LIMIT 1""").fetchone()
            if obsolete_rule:
                raise ValueError(f"Manual boundary no longer exists in CSV: {obsolete_rule}")

            # Find consecutive reporting periods for each title. A gap of exactly
            # 180 days stays in the same period; 181 days starts a new one.
            con.execute(f"""
                CREATE TEMP TABLE stage_dates AS
                WITH dated AS (
                    SELECT source_title, revenue_date,
                           lag(revenue_date) OVER (
                               PARTITION BY source_title ORDER BY revenue_date
                           ) AS previous_date
                    FROM (SELECT DISTINCT source_title, revenue_date FROM stage_revenues)
                )
                SELECT dated.source_title, dated.revenue_date,
                       sum(CASE WHEN previous_date IS NULL
                                   OR rule.action = 'split'
                                   OR (rule.action IS NULL AND
                                       date_diff('day', previous_date, dated.revenue_date) > {RUN_GAP_DAYS})
                                THEN 1 ELSE 0 END) OVER (
                           PARTITION BY dated.source_title ORDER BY dated.revenue_date
                       ) AS run_number
                FROM dated
                LEFT JOIN title_boundary_rule rule
                  ON rule.source_title = dated.source_title
                 AND rule.boundary_date = dated.revenue_date
            """)
            con.execute("""
                CREATE TEMP TABLE stage_runs AS
                SELECT source_title, run_number,
                       min(revenue_date) AS run_start_date,
                       max(revenue_date) AS run_end_date
                FROM stage_dates GROUP BY source_title, run_number
            """)
            reconciliation, boundary_moves, assignments, reset_keys = _reconcile_runs(
                con, boundary_change[0] if boundary_change else None)
            # Replace the source snapshot and fact within one transaction. Existing
            # run metadata stays attached to the same (title, start date) on reruns.
            con.execute("DELETE FROM fact_daily_revenue")
            con.execute("DELETE FROM raw_revenues")
            con.execute("INSERT INTO raw_revenues SELECT * FROM stage_revenues")
            con.execute("""CREATE TEMP TABLE stage_assignment (
                source_title VARCHAR, run_start_date DATE,
                run_end_date DATE, movie_key BIGINT)""")
            con.executemany("INSERT INTO stage_assignment VALUES (?, ?, ?, ?)",
                            assignments)
            anchor_collision = con.execute("""SELECT COUNT(*) FROM stage_assignment a
                JOIN dim_movie m ON m.source_title = a.source_title
                                AND m.run_start_date = a.run_start_date
                WHERE a.movie_key IS NULL""").fetchone()[0]
            if anchor_collision:
                raise RuntimeError("Unassigned period collides with a historical movie anchor")

            con.execute(
                """
                INSERT INTO dim_date
                SELECT CAST(strftime(revenue_date, '%Y%m%d') AS INTEGER),
                       revenue_date,
                       year(revenue_date),
                       quarter(revenue_date),
                       month(revenue_date),
                       isodow(revenue_date)
                FROM (SELECT DISTINCT revenue_date FROM stage_revenues) s
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_date d WHERE d.calendar_date = s.revenue_date
                )
                """
            )
            con.execute(
                """
                INSERT INTO dim_movie (movie_key, source_title, run_start_date,
                                       run_end_date, display_title)
                SELECT nextval('movie_key_seq'), s.source_title, s.run_start_date,
                       s.run_end_date, s.source_title
                FROM stage_assignment s WHERE s.movie_key IS NULL
                ORDER BY s.source_title, s.run_start_date
                """
            )
            con.execute("""
                UPDATE stage_assignment a SET movie_key = m.movie_key
                FROM dim_movie m WHERE a.movie_key IS NULL
                  AND m.source_title = a.source_title
                  AND m.run_start_date = a.run_start_date
            """)
            if con.execute("SELECT COUNT(*) FROM stage_assignment WHERE movie_key IS NULL").fetchone()[0]:
                raise RuntimeError("Could not assign every reporting period to a movie_key")
            if con.execute("""SELECT COUNT(*) FROM (
                SELECT movie_key FROM stage_assignment GROUP BY movie_key HAVING COUNT(*) > 1
            )""").fetchone()[0]:
                raise RuntimeError("Several reporting periods share one movie_key")
            con.execute("DELETE FROM dim_release_run")
            con.execute("""
                INSERT INTO dim_release_run
                SELECT movie_key, run_start_date, run_end_date,
                       CASE WHEN COUNT(*) OVER (PARTITION BY source_title) > 1
                       THEN source_title || ' (' || CASE
                           WHEN COUNT(*) OVER (
                               PARTITION BY source_title, year(run_start_date)
                           ) > 1 THEN CAST(run_start_date AS VARCHAR)
                           ELSE CAST(year(run_start_date) AS VARCHAR)
                       END || ')' ELSE source_title END
                FROM stage_assignment
            """)
            for movie_key, old_start, old_end, new_start, new_end, status in boundary_moves:
                con.execute("""
                    INSERT INTO run_boundary_change_log VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, [str(uuid4()), movie_key, old_start, old_end,
                       new_start, new_end, status,
                       datetime.now(timezone.utc).replace(tzinfo=None)])
            con.execute(
                """
                INSERT INTO dim_distributor
                SELECT nextval('distributor_key_seq'), s.distributor_name
                FROM (SELECT DISTINCT distributor_name FROM stage_revenues) s
                WHERE NOT EXISTS (
                    SELECT 1 FROM dim_distributor d
                    WHERE d.distributor_name = s.distributor_name
                )
                ORDER BY s.distributor_name
                """
            )
            con.execute(
                """
                INSERT INTO fact_daily_revenue
                SELECT r.source_id, d.date_key, a.movie_key, x.distributor_key,
                       r.revenue, r.theaters
                FROM raw_revenues r
                JOIN dim_date d ON d.calendar_date = r.revenue_date
                JOIN stage_dates sd ON sd.source_title = r.source_title
                                   AND sd.revenue_date = r.revenue_date
                JOIN stage_runs sr ON sr.source_title = sd.source_title
                                  AND sr.run_number = sd.run_number
                JOIN stage_assignment a ON a.source_title = sr.source_title
                                       AND a.run_start_date = sr.run_start_date
                JOIN dim_distributor x ON x.distributor_name = r.distributor_name
                """
            )

            loaded = con.execute(
                """
                SELECT COUNT(*), SUM(revenue),
                       COUNT(*) FILTER (WHERE theaters IS NULL)
                FROM fact_daily_revenue
                """
            ).fetchone()
            if loaded != (row_count, total_revenue, missing_theaters):
                raise RuntimeError(f"Fact reconciliation failed: source={source}, fact={loaded}")

            for key in reset_keys:
                con.execute("""UPDATE dim_movie SET imdb_id = NULL, omdb_title = NULL,
                    release_year = NULL, primary_genre = NULL, imdb_rating = NULL,
                    runtime_minutes = NULL, match_status = 'pending'
                    WHERE movie_key = ?""", [key])

            films = sync_films(con)

            summary = con.execute(
                """
                SELECT (SELECT COUNT(*) FROM fact_daily_revenue) AS fact_rows,
                       (SELECT COUNT(DISTINCT movie_key) FROM fact_daily_revenue) AS movies,
                       (SELECT COUNT(*) FROM dim_distributor) AS distributors,
                       (SELECT COUNT(*) FROM dim_date) AS dates,
                       (SELECT MIN(revenue_date) FROM raw_revenues) AS first_date,
                       (SELECT MAX(revenue_date) FROM raw_revenues) AS last_date
                """
            ).fetchone()
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.close()

    return {
        "fact_rows": summary[0],
        "movies": summary[1],
        "distributors": summary[2],
        "dates": summary[3],
        "first_date": str(summary[4]),
        "last_date": str(summary[5]),
        "total_revenue": total_revenue,
        "missing_theaters": missing_theaters,
        **reconciliation,
        **films,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv", type=Path, default=ROOT / "revenues_per_day.csv")
    parser.add_argument("--db", type=Path, default=ROOT / "data" / "box_office.duckdb")
    args = parser.parse_args()
    print(json.dumps(load(args.csv, args.db), indent=2))


if __name__ == "__main__":
    main()
