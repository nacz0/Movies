import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from dashboard.queries import distributor_ranking, film_ranking, movie_ranking, overview
from pipeline.enrich_omdb import enrich
from pipeline.load_revenues import load


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.csv = Path(self.tmp.name) / "revenue.csv"
        self.db = Path(self.tmp.name) / "warehouse.duckdb"
        self.write_rows([
            ["1", "2020-01-01", "Film A", "100", "10", "Studio X"],
            ["2", "2021-01-01", "Film A", "50", "8", "Studio X"],
            ["3", "2021-01-02", "Film B", "25", "2", "Studio Y"],
        ])

    def write_rows(self, rows):
        with self.csv.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["id", "date", "title", "revenue", "theaters", "distributor"])
            writer.writerows(rows)

    def test_import_reconciles_and_rolls_back_invalid_csv(self):
        summary = load(self.csv, self.db)
        self.assertEqual((summary["fact_rows"], summary["periods"], summary["total_revenue"]),
                         (3, 3, 175))
        self.write_rows([["4", "2022-01-01", "Bad", "-1", "1", "Studio X"]])
        with self.assertRaises(ValueError):
            load(self.csv, self.db)
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute("SELECT count(*), sum(revenue) FROM fact_daily_revenue").fetchone(),
                             (3, 175))

    def test_saved_response_survives_reload_and_combines_periods(self):
        load(self.csv, self.db)
        payload = {
            "Response": "True", "Type": "movie", "Title": "Film A",
            "Year": "2020", "imdbID": "tt1234567", "Genre": "Drama",
            "imdbRating": "7.2", "Runtime": "100 min",
        }
        result = enrich(self.db, "test-key", limit=1, fetcher=lambda *_: payload)
        self.assertEqual(result["requests"], 1)
        self.assertEqual(result["cache_reused"], 1)
        with duckdb.connect(str(self.db)) as con:
            saved = con.execute("SELECT movie_key, response_json FROM omdb_lookup ORDER BY movie_key").fetchall()
        load(self.csv, self.db)
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute(
                "SELECT movie_key, response_json FROM omdb_lookup ORDER BY movie_key"
            ).fetchall(), saved)
            self.assertEqual(con.execute("SELECT count(*) FROM omdb_request_log").fetchone()[0], 1)
            con.execute("UPDATE dim_movie SET match_status = 'pending' WHERE source_title = 'Film A'")
        offline = enrich(self.db, None, limit=1, fetcher=lambda *_: self.fail("network called"))
        self.assertEqual(offline["cache_reused"], 2)
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM omdb_request_log").fetchone()[0], 1)
            start, end = date(2020, 1, 1), date(2021, 1, 2)
            self.assertEqual(overview(con, start, end)["revenue"], 175)
            self.assertEqual([(row["film"], row["revenue"], row["periods"])
                              for row in film_ranking(con, start, end)],
                             [("Film A (2020)", 150, 2)])
            self.assertEqual(sum(row["revenue"] for row in movie_ranking(con, start, end)), 175)

    def test_invalid_key_stops_without_spending_more_requests(self):
        load(self.csv, self.db)
        invalid = {"Response": "False", "Error": "Invalid API key!"}
        result = enrich(self.db, "bad-key", limit=10, fetcher=lambda *_: invalid)
        self.assertEqual((result["requests"], result["invalid_key"]), (1, 1))
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM omdb_request_log").fetchone()[0], 1)
            self.assertEqual(con.execute(
                "SELECT count(*) FROM dim_movie WHERE match_status = 'pending'"
            ).fetchone()[0], 3)

    def test_sorting_happens_before_ranking_limit(self):
        load(self.csv, self.db)
        with duckdb.connect(str(self.db)) as con:
            start, end = date(2020, 1, 1), date(2021, 1, 2)
            self.assertEqual(movie_ranking(con, start, end, limit=1)[0]["revenue"], 100)
            self.assertEqual(movie_ranking(con, start, end, limit=1,
                                           sort="movie", direction="desc")[0]["movie"], "Film B")
            self.assertEqual(distributor_ranking(con, start, end, limit=1,
                                                 sort="distributor", direction="desc")[0]["distributor"],
                             "Studio Y")
            with self.assertRaises(ValueError):
                movie_ranking(con, start, end, sort="revenue; DROP TABLE dim_movie")


if __name__ == "__main__":
    unittest.main()
