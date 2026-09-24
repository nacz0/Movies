import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from pipeline.enrich_omdb import enrich
from pipeline.load_revenues import load
from pipeline.review_boundaries import list_rules
from pipeline.review_matches import decide


class BoundaryReviewTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        root = Path(self.tempdir.name)
        self.csv_path = root / "revenues.csv"
        self.db_path = root / "warehouse.duckdb"
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows([
                ["1", "2020-01-01", "Film", "10", "1", "Studio"],
                ["2", "2020-02-01", "Film", "20", "1", "Studio"],
                ["3", "2020-03-01", "Film", "30", "1", "Studio"],
            ])
        load(self.csv_path, self.db_path)

    def test_split_after_earlier_date_keeps_distinct_keys_and_is_reversible(self) -> None:
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows([
                ["1", "2020-02-01", "Film", "20", "1", "Studio"],
                ["2", "2020-03-01", "Film", "30", "1", "Studio"],
            ])
        self.db_path.unlink()
        load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            original_key = con.execute("SELECT movie_key FROM dim_movie").fetchone()[0]
        with self.csv_path.open("a", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow(["early", "2020-01-31", "Film", "10", "1", "Studio"])
        load(self.csv_path, self.db_path)
        result = load(self.csv_path, self.db_path,
                      boundary_change=("Film", date(2020, 2, 1), "split",
                                       "Earlier date belongs to another release"))
        self.assertEqual((result["fact_rows"], result["total_revenue"],
                          result["movies"]), (3, 60, 2))
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            rows = con.execute("""SELECT r.run_start_date, r.movie_key,
                SUM(f.revenue) FROM dim_release_run r
                JOIN fact_daily_revenue f USING (movie_key)
                GROUP BY r.run_start_date, r.movie_key
                ORDER BY r.run_start_date""").fetchall()
            self.assertEqual([row[2] for row in rows], [10, 50])
            self.assertNotEqual(rows[0][1], rows[1][1])
            self.assertEqual(rows[1][1], original_key)
        self.assertEqual(load(self.csv_path, self.db_path)["movies"], 2)
        self.assertEqual(load(self.csv_path, self.db_path,
                              boundary_change=("Film", date(2020, 2, 1), None,
                                               "Correction withdrawn"))["movies"], 1)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT SUM(revenue) FROM fact_daily_revenue").fetchone()[0], 60)

    def test_failed_split_after_extension_rolls_back_rule_and_history(self) -> None:
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows([
                ["1", "2020-02-01", "Film", "20", "1", "Studio"],
                ["2", "2020-03-01", "Film", "30", "1", "Studio"],
            ])
        self.db_path.unlink()
        load(self.csv_path, self.db_path)
        with self.csv_path.open("a", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow(["early", "2020-01-31", "Film", "10", "1", "Studio"])
        load(self.csv_path, self.db_path)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            key = con.execute("SELECT movie_key FROM dim_release_run").fetchone()[0]
        decide(self.db_path, key, "verify_id", "Checked credits", "tt7654321")
        with self.assertRaisesRegex(ValueError, "Undo manual match decisions"):
            load(self.csv_path, self.db_path,
                 boundary_change=("Film", date(2020, 2, 1), "split",
                                  "Earlier date belongs to another release"))
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM title_boundary_rule").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM title_boundary_rule_event").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*), SUM(revenue) FROM fact_daily_revenue").fetchone(), (3, 60))

    def test_split_persists_and_remove_restores_without_losing_facts(self) -> None:
        def fake_fetch(title: str, key: str) -> dict:
            return {"Response": "True", "Type": "movie", "imdbID": "tt1234567",
                    "Title": title, "Year": "2020"}
        enrich(self.db_path, "fake", limit=1, daily_cap=1, fetcher=fake_fetch)
        result = load(self.csv_path, self.db_path,
                      boundary_change=("Film", date(2020, 2, 1), "split",
                                       "Two distinct theatrical releases"))
        self.assertEqual(result["movies"], 2)
        self.assertEqual(result["total_revenue"], 60)
        self.assertEqual(len(list_rules(self.db_path)), 1)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""SELECT match_status FROM dim_movie m
                JOIN dim_release_run r USING (movie_key)
                ORDER BY r.run_start_date""").fetchall(),
                [("pending",), ("pending",)])
            self.assertEqual(con.execute("""SELECT COUNT(*) FROM omdb_request_log""").fetchone()[0], 1)
            self.assertEqual(con.execute("""SELECT COUNT(*) FROM v_movie_match_review""").fetchone()[0], 2)
        self.assertEqual(load(self.csv_path, self.db_path)["movies"], 2)
        restored = load(self.csv_path, self.db_path,
                        boundary_change=("Film", date(2020, 2, 1), None,
                                         "Correction withdrawn"))
        self.assertEqual(restored["movies"], 1)
        self.assertEqual(list_rules(self.db_path), [])
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT SUM(revenue) FROM fact_daily_revenue").fetchone()[0], 60)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM title_boundary_rule_event").fetchone()[0], 2)

    def test_manual_match_blocks_split_and_rolls_back_rule(self) -> None:
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            key = con.execute("SELECT movie_key FROM dim_release_run").fetchone()[0]
        decide(self.db_path, key, "verify_id", "Checked credits", "tt7654321")
        with self.assertRaisesRegex(ValueError, "Undo manual match decisions"):
            load(self.csv_path, self.db_path,
                 boundary_change=("Film", date(2020, 2, 1), "split",
                                  "Two distinct releases"))
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM title_boundary_rule").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM fact_daily_revenue").fetchone()[0], 3)

    def test_join_distant_periods(self) -> None:
        with self.csv_path.open("a", newline="", encoding="utf-8") as handle:
            csv.writer(handle).writerow(["4", "2021-01-01", "Film", "40", "1", "Studio"])
        self.assertEqual(load(self.csv_path, self.db_path)["movies"], 2)
        result = load(self.csv_path, self.db_path,
                      boundary_change=("Film", date(2021, 1, 1), "join",
                                       "Same film reappeared"))
        self.assertEqual(result["movies"], 1)
        self.assertEqual(result["total_revenue"], 100)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(DISTINCT movie_key) FROM fact_daily_revenue").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
