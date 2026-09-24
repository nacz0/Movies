import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from pipeline.load_revenues import load
from pipeline.review_matches import decide


FIELDS = ["id", "date", "title", "revenue", "theaters", "distributor"]
ROWS = [
    ["a", "2023-01-01", "Film A", "100", "10", "Studio X"],
    ["b", "2023-01-02", "Film A", "50", "", "Studio X"],
    ["c", "2023-01-02", "Film B", "25", "2", "Studio Y"],
]


def write_csv(path: Path, rows: list[list[str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(FIELDS)
        writer.writerows(rows)


class LoadRevenuesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        self.root = Path(self.tempdir.name)
        self.csv_path = self.root / "revenues.csv"
        self.db_path = self.root / "warehouse.duckdb"
        write_csv(self.csv_path, ROWS)

    def test_load_and_rerun_preserve_enrichment(self) -> None:
        first = load(self.csv_path, self.db_path)
        self.assertEqual(first["fact_rows"], 3)
        self.assertEqual(first["total_revenue"], 175)
        self.assertEqual(first["missing_theaters"], 1)
        self.assertEqual(first["movies"], 2)

        with duckdb.connect(str(self.db_path)) as con:
            con.execute(
                """UPDATE dim_movie SET imdb_id = 'tt1234567',
                   match_status = 'matched' WHERE source_title = 'Film A'"""
            )

        second = load(self.csv_path, self.db_path)
        self.assertEqual(second["fact_rows"], first["fact_rows"])
        self.assertEqual(second["total_revenue"], first["total_revenue"])
        self.assertEqual(second["movies"], first["movies"])
        self.assertEqual(second["confirmed_films"], 1)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue").fetchone(),
                (3, 175),
            )
            self.assertEqual(
                con.execute(
                    "SELECT imdb_id, match_status FROM dim_movie "
                    "WHERE source_title = 'Film A'"
                ).fetchone(),
                ("tt1234567", "matched"),
            )

    def test_invalid_snapshot_rolls_back_previous_load(self) -> None:
        load(self.csv_path, self.db_path)
        write_csv(self.csv_path, ROWS + [["d", "2023-01-03", "Film C", "-1", "4", "Studio Z"]])

        with self.assertRaisesRegex(ValueError, "invalid required values"):
            load(self.csv_path, self.db_path)

        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue").fetchone(),
                (3, 175),
            )

    def test_long_gap_splits_same_title_without_losing_revenue(self) -> None:
        write_csv(self.csv_path, [
            ["a", "2010-01-01", "Frozen", "10", "1", "Studio X"],
            ["b", "2010-06-30", "Frozen", "20", "1", "Studio X"],  # 180 days
            ["c", "2010-12-28", "Frozen", "30", "1", "Studio X"],  # 181 days
            ["d", "2013-11-27", "Frozen", "40", "1", "Studio X"],
        ])
        result = load(self.csv_path, self.db_path)
        self.assertEqual(result["movies"], 3)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            rows = con.execute("""
                SELECT m.run_start_date, m.run_end_date, SUM(f.revenue)
                FROM dim_movie m JOIN fact_daily_revenue f USING (movie_key)
                GROUP BY m.movie_key, m.run_start_date, m.run_end_date
                ORDER BY m.run_start_date
            """).fetchall()
            self.assertEqual([row[2] for row in rows], [30, 30, 40])
            self.assertEqual(sum(row[2] for row in rows), 100)
            self.assertEqual(rows[0][1].isoformat(), "2010-06-30")

    def test_earlier_record_preserves_run_key_and_review(self) -> None:
        write_csv(self.csv_path, [
            ["a", "2020-02-01", "Film A", "100", "10", "Studio X"],
            ["b", "2020-02-02", "Film A", "50", "10", "Studio X"],
        ])
        load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            original_key = con.execute("SELECT movie_key FROM dim_movie").fetchone()[0]
        decide(self.db_path, original_key, "verify_id", "Checked credits", "tt1234567")

        write_csv(self.csv_path, [
            ["early", "2020-01-31", "Film A", "20", "10", "Studio X"],
            ["a", "2020-02-01", "Film A", "100", "10", "Studio X"],
            ["b", "2020-02-02", "Film A", "50", "10", "Studio X"],
        ])
        result = load(self.csv_path, self.db_path)
        self.assertEqual(result["boundary_changes"], 1)
        self.assertEqual(result["movies"], 1)
        self.assertEqual(load(self.csv_path, self.db_path)["boundary_changes"], 0)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""
                SELECT m.movie_key, r.run_start_date, m.imdb_id, m.match_status
                FROM dim_movie m JOIN dim_release_run r USING (movie_key)
                WHERE m.movie_key = ?
            """, [original_key]).fetchone(),
                             (original_key, date(2020, 1, 31),
                              "tt1234567", "verified_id"))
            self.assertEqual(con.execute("""
                SELECT COUNT(*) FROM movie_match_decision WHERE movie_key = ?
            """, [original_key]).fetchone()[0], 1)
            self.assertEqual(con.execute("""
                SELECT COUNT(*) FROM run_boundary_change_log WHERE movie_key = ?
            """, [original_key]).fetchone()[0], 1)
            self.assertEqual(con.execute("""
                SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue
                WHERE movie_key = ?
            """, [original_key]).fetchone(), (3, 170))

    def test_split_of_reviewed_run_aborts_without_changing_snapshot(self) -> None:
        original_rows = [
            ["a", "2020-01-01", "Film A", "100", "10", "Studio X"],
            ["bridge", "2020-04-01", "Film A", "50", "10", "Studio X"],
            ["b", "2020-08-01", "Film A", "25", "10", "Studio X"],
        ]
        write_csv(self.csv_path, original_rows)
        load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            original_key = con.execute("SELECT movie_key FROM dim_movie").fetchone()[0]
        decide(self.db_path, original_key, "verify_id", "Checked credits", "tt1234567")
        write_csv(self.csv_path, [original_rows[0], original_rows[2]])

        with self.assertRaisesRegex(ValueError, "split/merge"):
            load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""
                SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue
            """).fetchone(), (3, 175))
            self.assertEqual(con.execute("""
                SELECT imdb_id, match_status FROM dim_movie WHERE movie_key = ?
            """, [original_key]).fetchone(), ("tt1234567", "verified_id"))

    def test_merge_of_reviewed_runs_aborts_without_changing_snapshot(self) -> None:
        original_rows = [
            ["a", "2020-01-01", "Film A", "100", "10", "Studio X"],
            ["b", "2020-08-01", "Film A", "25", "10", "Studio X"],
        ]
        write_csv(self.csv_path, original_rows)
        load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            first_key = con.execute("""
                SELECT movie_key FROM dim_release_run ORDER BY run_start_date LIMIT 1
            """).fetchone()[0]
        decide(self.db_path, first_key, "verify_id", "Checked credits", "tt1234567")
        write_csv(self.csv_path, [
            original_rows[0],
            ["bridge", "2020-04-01", "Film A", "50", "10", "Studio X"],
            original_rows[1],
        ])
        with self.assertRaisesRegex(ValueError, "split/merge"):
            load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""
                SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue
            """).fetchone(), (2, 125))
            self.assertEqual(con.execute("SELECT COUNT(*) FROM dim_release_run").fetchone()[0], 2)


if __name__ == "__main__":
    unittest.main()
