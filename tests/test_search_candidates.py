import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from pipeline.enrich_omdb import enrich
from pipeline.load_revenues import load
from pipeline.review_matches import decide, undo
from pipeline.search_candidates import (
    hydrate_verified, list_candidates, search_candidates, set_alias,
)


class AlternativeCandidateTests(unittest.TestCase):
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
        def title_fetch(title: str, key: str) -> dict:
            return {"Response": "True", "Type": "movie", "imdbID": "tt2294629",
                    "Title": title, "Year": "2013", "Genre": "Animation"}
        enrich(self.db_path, "fake-key", limit=1, daily_cap=3, fetcher=title_fetch)
        enrich(self.db_path, "fake-key", limit=1, daily_cap=3, fetcher=title_fetch)
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.old_key = con.execute("""SELECT movie_key FROM dim_release_run
                ORDER BY run_start_date LIMIT 1""").fetchone()[0]

    def test_alias_search_select_and_hydrate_respect_shared_budget(self) -> None:
        set_alias(self.db_path, "Frozen", "Frozen 2010", "Reviewer checked source listing")
        calls = []
        def search_fetch(title: str, year: int | None, key: str) -> dict:
            calls.append((title, year))
            return {"Response": "True", "Search": [
                {"Title": "Frozen", "Year": "2010", "imdbID": "tt1323045", "Type": "movie"},
                {"Title": "Frozen II", "Year": "2019", "imdbID": "tt4520988", "Type": "movie"},
            ]}
        first = search_candidates(self.db_path, "fake-key", movie_key=self.old_key,
                                  search_title="Frozen 2010", limit=1, daily_cap=2,
                                  fetcher=search_fetch)
        self.assertEqual(first["candidates_saved"], 2)
        self.assertEqual(calls, [("Frozen 2010", 2010)])
        second = search_candidates(self.db_path, "fake-key", movie_key=self.old_key,
                                   search_title="Frozen 2010", limit=1, daily_cap=2,
                                   fetcher=search_fetch)
        self.assertEqual(second["cache_reused"], 1)
        self.assertEqual(len(list_candidates(self.db_path, self.old_key)), 2)

        selected = decide(self.db_path, self.old_key, "verify_id",
                          "Reviewed candidate credits", "tt1323045")
        self.assertEqual(selected["match_status"], "verified_id")
        detail_calls = []
        def detail_fetch(imdb_id: str, key: str) -> dict:
            detail_calls.append(imdb_id)
            return {"Response": "True", "Type": "movie", "imdbID": imdb_id,
                    "Title": "Frozen", "Year": "2010", "Genre": "Horror",
                    "imdbRating": "6.1", "Runtime": "93 min"}
        blocked = hydrate_verified(self.db_path, "fake-key", daily_cap=2,
                                   movie_key=self.old_key,
                                   fetcher=detail_fetch)
        self.assertEqual(blocked["budget_stop"], 1)
        self.assertEqual(detail_calls, [])
        result = hydrate_verified(self.db_path, "fake-key", daily_cap=3,
                                  movie_key=self.old_key,
                                  fetcher=detail_fetch)
        self.assertEqual(result["hydrated"], 1)
        self.assertEqual(detail_calls, ["tt1323045"])
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("""SELECT imdb_id, primary_genre, match_status
                FROM dim_movie WHERE movie_key = ?""", [self.old_key]).fetchone(),
                ("tt1323045", "Horror", "matched"))
            self.assertEqual(con.execute("""SELECT candidate_imdb_id FROM omdb_lookup
                WHERE movie_key = ?""", [self.old_key]).fetchone()[0], "tt2294629")
            self.assertEqual(con.execute("""SELECT request_kind FROM omdb_request_log
                ORDER BY request_kind""").fetchall(),
                [("detail",), ("search",), ("title",)])
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 3)
        self.assertEqual(undo(self.db_path, self.old_key)["match_status"], "ambiguous")

    def test_unregistered_alias_does_not_spend_budget(self) -> None:
        with self.assertRaisesRegex(ValueError, "Register this alias"):
            search_candidates(self.db_path, "fake-key", movie_key=self.old_key,
                              search_title="Unregistered", fetcher=lambda *_: {})
        with duckdb.connect(str(self.db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 1)

    def test_batch_search_skips_cached_first_run_and_reaches_second(self) -> None:
        root = Path(self.tempdir.name)
        csv_path = root / "batch.csv"
        db_path = root / "batch.duckdb"
        with csv_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows([
                ["1", "2020-01-01", "Film A", "100", "1", "Studio"],
                ["2", "2020-01-01", "Film B", "50", "1", "Studio"],
            ])
        load(csv_path, db_path)
        enrich(db_path, "fake", limit=2, daily_cap=5,
               fetcher=lambda *_: {"Response": "False", "Error": "Movie not found!"})
        calls = []
        def fake_search(title: str, year: int | None, key: str) -> dict:
            calls.append((title, year))
            return {"Response": "False", "Error": "Movie not found!"}
        first = search_candidates(db_path, "fake", limit=1, daily_cap=3,
                                  fetcher=fake_search)
        self.assertEqual(first["not_found"], 1)
        second = search_candidates(db_path, "fake", limit=1, daily_cap=4,
                                   fetcher=fake_search)
        self.assertEqual(second["cache_reused"], 1)
        self.assertEqual(second["not_found"], 1)
        third = search_candidates(db_path, "fake", limit=1, daily_cap=4,
                                  fetcher=fake_search)
        self.assertEqual(third["cache_reused"], 2)
        self.assertEqual(calls, [("Film A", 2020), ("Film B", 2020)])
        with duckdb.connect(str(db_path), read_only=True) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM omdb_request_log").fetchone()[0], 4)

        with duckdb.connect(str(db_path), read_only=True) as con:
            key = con.execute("""SELECT movie_key FROM dim_movie
                WHERE source_title = 'Film A'""").fetchone()[0]
        refreshed = search_candidates(db_path, "fake", limit=1, daily_cap=5,
                                      movie_key=key, refresh=True,
                                      fetcher=fake_search)
        self.assertEqual(refreshed["not_found"], 1)
        self.assertEqual(len(calls), 3)


if __name__ == "__main__":
    unittest.main()
