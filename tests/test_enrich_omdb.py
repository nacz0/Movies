import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from pipeline.enrich_omdb import classify, enrich
from pipeline.load_revenues import load


class EnrichOmdbTests(unittest.TestCase):
    def test_remake_and_long_running_title_require_review(self) -> None:
        candidate = {
            "Response": "True", "Type": "movie", "imdbID": "tt0110357",
            "Title": "The Lion King", "Year": "1994",
        }
        self.assertEqual(
            classify(candidate, "The Lion King", 2019, 2019)[0], "ambiguous"
        )
        candidate["Year"] = "2019"
        self.assertEqual(
            classify(candidate, "The Lion King", 2019, 2019)[0], "matched"
        )
        self.assertEqual(
            classify(candidate, "The Lion King", 2019, 2023)[0], "ambiguous"
        )

    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tempdir.cleanup)
        root = Path(self.tempdir.name)
        self.csv_path = root / "revenues.csv"
        self.db_path = root / "warehouse.duckdb"
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows(
                [
                    ["1", "2022-01-01", "Film A", "100", "10", "Studio"],
                    ["2", "2022-01-01", "Film B", "80", "8", "Studio"],
                    ["3", "2022-01-01", "Film C", "60", "6", "Studio"],
                ]
            )
        load(self.csv_path, self.db_path)

    def test_small_budget_caches_matches_and_flags_ambiguity(self) -> None:
        calls = []

        def fake_fetch(title: str, api_key: str) -> dict:
            calls.append(title)
            return {
                "Response": "True",
                "Type": "movie",
                "imdbID": "tt1234567",
                "Title": title if title == "Film A" else "Another Film",
                "Year": "2021",
                "Genre": "Drama, Comedy",
                "imdbRating": "7.5",
                "Runtime": "101 min",
            }

        first = enrich(self.db_path, "test-key", limit=2, daily_cap=2, fetcher=fake_fetch)
        self.assertEqual(calls, ["Film A", "Film B"])
        self.assertEqual(first["matched"], 1)
        self.assertEqual(first["ambiguous"], 1)
        self.assertEqual(first["requests_last_24h"], 2)

        second = enrich(self.db_path, "test-key", limit=2, daily_cap=2, fetcher=fake_fetch)
        self.assertEqual(calls, ["Film A", "Film B"])
        self.assertEqual(second["budget_stop"], 1)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(
                con.execute(
                    "SELECT imdb_id, primary_genre, imdb_rating, runtime_minutes "
                    "FROM dim_movie WHERE source_title = 'Film A'"
                ).fetchone(),
                ("tt1234567", "Drama", 7.5, 101),
            )
            self.assertEqual(
                con.execute(
                    "SELECT imdb_id, match_status FROM dim_movie "
                    "WHERE source_title = 'Film B'"
                ).fetchone(),
                (None, "ambiguous"),
            )
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 2)

    def test_api_key_error_stops_and_leaves_title_pending(self) -> None:
        calls = []

        def bad_key(title: str, api_key: str) -> dict:
            calls.append(title)
            return {"Response": "False", "Error": "Invalid API key!"}

        result = enrich(self.db_path, "test-key", limit=3, daily_cap=3, fetcher=bad_key)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["invalid_key"], 1)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(
                con.execute("SELECT COUNT(*) FROM dim_movie WHERE match_status = 'pending'").fetchone()[0],
                3,
            )
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 1)

        def good_key(title: str, api_key: str) -> dict:
            calls.append(title)
            return {"Response": "True", "Type": "movie", "imdbID": "tt1234567",
                    "Title": title, "Year": "2021"}

        resumed = enrich(self.db_path, "test-key", limit=1, daily_cap=2, fetcher=good_key)
        self.assertEqual(resumed["matched"], 1)
        self.assertEqual(calls, ["Film A", "Film A"])
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 2)
            self.assertEqual(con.execute("""
                SELECT match_status FROM dim_movie WHERE source_title = 'Film A'
            """).fetchone()[0], "matched")

    def test_quota_error_can_resume_with_budget(self) -> None:
        calls = []
        def quota(title: str, key: str) -> dict:
            calls.append(title)
            return {"Response": "False", "Error": "Request limit reached!"}
        first = enrich(self.db_path, "fake", limit=2, daily_cap=1, fetcher=quota)
        self.assertEqual(first["quota_exhausted"], 1)
        def found(title: str, key: str) -> dict:
            calls.append(title)
            return {"Response": "True", "Type": "movie", "imdbID": "tt1234567",
                    "Title": title, "Year": "2022"}
        blocked = enrich(self.db_path, "fake", limit=1, daily_cap=1, fetcher=found)
        self.assertEqual(blocked["budget_stop"], 1)
        resumed = enrich(self.db_path, "fake", limit=1, daily_cap=2, fetcher=found)
        self.assertEqual(resumed["matched"], 1)
        self.assertEqual(calls, ["Film A", "Film A"])

    def test_not_found_refresh_is_explicit_and_budgeted(self) -> None:
        calls = []
        def not_found(title: str, key: str) -> dict:
            calls.append(title)
            return {"Response": "False", "Error": "Movie not found!"}
        first = enrich(self.db_path, "fake", limit=1, daily_cap=2,
                       fetcher=not_found)
        self.assertEqual(first["not_found"], 1)
        def found(title: str, key: str) -> dict:
            calls.append(title)
            return {"Response": "True", "Type": "movie", "imdbID": "tt1234567",
                    "Title": title, "Year": "2022"}
        enrich(self.db_path, "fake", limit=1, daily_cap=2, fetcher=found)
        self.assertEqual(calls, ["Film A", "Film B"])
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            key = con.execute("""SELECT movie_key FROM dim_movie
                WHERE source_title = 'Film A'""").fetchone()[0]
        refreshed = enrich(self.db_path, "fake", limit=1, daily_cap=3,
                           retry_not_found=True, movie_key=key, fetcher=found)
        self.assertEqual(refreshed["matched"], 1)
        self.assertEqual(calls, ["Film A", "Film B", "Film A"])

    def test_second_run_reuses_saved_title_response_without_request(self) -> None:
        with self.csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows([
                ["1", "2010-01-01", "Frozen", "10", "1", "Studio"],
                ["2", "2013-11-27", "Frozen", "100", "1", "Studio"],
            ])
        load(self.csv_path, self.db_path)
        calls = []

        def fake_fetch(title: str, api_key: str) -> dict:
            calls.append(title)
            return {"Response": "True", "Type": "movie", "imdbID": "tt2294629",
                    "Title": "Frozen", "Year": "2013"}

        enrich(self.db_path, "test-key", limit=1, daily_cap=1, fetcher=fake_fetch)
        result = enrich(self.db_path, "test-key", limit=1, daily_cap=1, fetcher=fake_fetch)
        self.assertEqual(calls, ["Frozen"])
        self.assertEqual(result["cache_reused"], 1)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 1)
            self.assertEqual(con.execute("""
                SELECT match_status FROM dim_movie WHERE source_title = 'Frozen'
                ORDER BY run_start_date
            """).fetchall(), [("ambiguous",), ("matched",)])


if __name__ == "__main__":
    unittest.main()
