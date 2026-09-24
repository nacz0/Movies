import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from dashboard.queries import review_cases
from pipeline.enrich_omdb import enrich
from pipeline.load_revenues import load
from pipeline.review_matches import decide, list_cases, undo


class ReviewMatchesTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        root = Path(self.tempdir.name)
        self.db_path = root / "warehouse.duckdb"
        csv_path = root / "revenues.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows([
                ["1", "2010-02-05", "Frozen", "10", "1", "Studio"],
                ["2", "2013-11-22", "Frozen", "100", "1", "Studio"],
            ])
        load(csv_path, self.db_path)

        def fake_fetch(title: str, api_key: str) -> dict:
            return {"Response": "True", "Type": "movie", "imdbID": "tt2294629",
                    "Title": "Frozen", "Year": "2013", "Genre": "Animation",
                    "imdbRating": "7.4", "Runtime": "102 min"}

        enrich(self.db_path, "test-key", limit=1, daily_cap=1, fetcher=fake_fetch)
        enrich(self.db_path, "test-key", limit=1, daily_cap=1, fetcher=fake_fetch)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.old_key, self.new_key = [row[0] for row in con.execute(
                "SELECT movie_key FROM dim_movie ORDER BY run_start_date"
            ).fetchall()]

    def test_review_accept_and_undo_preserve_cached_response_and_request_count(self) -> None:
        cases = list_cases(self.db_path)
        self.assertEqual(len(cases), 2)
        self.assertEqual(cases[0]["movie_key"], self.old_key)
        self.assertEqual(cases[0]["match_status"], "ambiguous")
        self.assertEqual(cases[0]["candidate_imdb_id"], "tt2294629")

        result = decide(self.db_path, self.old_key, "accept_candidate",
                        "Human confirmed this candidate")
        self.assertEqual(result["match_status"], "matched")
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""
                SELECT m.imdb_id, m.primary_genre, m.match_status,
                       l.lookup_status, j.reason
                FROM dim_movie m JOIN omdb_lookup l USING (movie_key)
                JOIN movie_match_decision j USING (movie_key)
                WHERE m.movie_key = ?
            """, [self.old_key]).fetchone(),
                             ("tt2294629", "Animation", "matched", "ambiguous",
                              "Human confirmed this candidate"))
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 1)
            self.assertEqual(con.execute("SELECT SUM(revenue) FROM fact_daily_revenue").fetchone()[0], 110)
            self.assertEqual(review_cases(con)["total"], 2)

        self.assertEqual(undo(self.db_path, self.old_key)["match_status"], "ambiguous")
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""
                SELECT imdb_id, match_status FROM dim_movie WHERE movie_key = ?
            """, [self.old_key]).fetchone(), (None, "ambiguous"))
            self.assertEqual(con.execute("SELECT COUNT(*) FROM movie_match_decision").fetchone()[0], 0)

    def test_verify_different_id_stays_distinct_from_omdb_metadata(self) -> None:
        with self.assertRaisesRegex(ValueError, "reason"):
            decide(self.db_path, self.old_key, "verify_id", " ", "tt1234567")
        result = decide(self.db_path, self.old_key, "verify_id",
                        "Checked film credits", "tt1234567")
        self.assertEqual(result["match_status"], "verified_id")
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""
                SELECT imdb_id, primary_genre, match_status FROM dim_movie
                WHERE movie_key = ?
            """, [self.old_key]).fetchone(),
                             ("tt1234567", None, "verified_id"))
        self.assertEqual(len(list_cases(self.db_path, unresolved_only=True)), 0)


if __name__ == "__main__":
    unittest.main()
