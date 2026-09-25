import csv
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

import duckdb

from dashboard.queries import distributor_ranking, film_ranking, movie_ranking, overview
from pipeline.enrich_omdb import enrich, fetch_title
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

    def prepare_remakes(self):
        self.write_rows([
            ['1', '2019-07-19', 'The Lion King', '100', '10', 'Studio X'],
            ['2', '2020-08-01', 'The Lion King', '50', '8', 'Studio X'],
        ])
        load(self.csv, self.db)

    @staticmethod
    def lion(year, title='The Lion King'):
        return {'Response': 'True', 'Type': 'movie', 'Title': title,
                'Year': str(year), 'imdbID': f'tt{year}000', 'Genre': 'Adventure'}

    def test_year_query_parameter_is_optional(self):
        with patch('pipeline.enrich_omdb.urllib.request.urlopen') as urlopen:
            urlopen.return_value.__enter__.return_value.read.return_value = b'{}'
            fetch_title('The Lion King', 'test-key', 2019)
            query = parse_qs(urlsplit(urlopen.call_args.args[0].full_url).query)
            self.assertEqual(query['y'], ['2019'])
            fetch_title('The Lion King', 'test-key')
            query = parse_qs(urlsplit(urlopen.call_args.args[0].full_url).query)
            self.assertNotIn('y', query)

    def test_remakes_use_separate_persistent_year_cache(self):
        self.prepare_remakes()
        calls = []
        def fetch(title, key, year):
            calls.append(year)
            return self.lion(year or 1994)
        result = enrich(self.db, 'test-key', limit=3, fetcher=fetch)
        self.assertEqual(calls, [None, 2019, 2020])
        self.assertEqual(result['requests'], 3)
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute(
                'SELECT release_year FROM dim_movie ORDER BY run_start_date'
            ).fetchall(), [(2019,), (2020,)])
            self.assertEqual(con.execute('SELECT query_year FROM omdb_query_cache ORDER BY 1').fetchall(),
                             [(0,), (2019,), (2020,)])
            con.execute("UPDATE dim_movie SET match_status='pending'")
        result = enrich(self.db, None, fetcher=lambda *_: self.fail('Network called'))
        self.assertEqual(result['cache_reused'], 4)
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute('SELECT count(*) FROM omdb_request_log').fetchone()[0], 3)
            self.assertEqual(con.execute('SELECT release_year FROM dim_movie ORDER BY run_start_date').fetchall(),
                             [(2019,), (2020,)])

    def test_fallback_respects_limits_and_can_resume_ambiguous(self):
        for cap in ('limit', 'daily_cap'):
            with self.subTest(cap=cap):
                self.db = Path(self.tmp.name) / f'{cap}.duckdb'
                self.prepare_remakes()
                result = enrich(self.db, 'test-key', **{cap: 1}, fetcher=lambda *_: self.lion(1994))
                self.assertEqual(result['requests'], 1)
                with duckdb.connect(str(self.db)) as con:
                    self.assertEqual(con.execute(
                        "SELECT count(*) FROM dim_movie WHERE match_status='ambiguous'"
                    ).fetchone()[0], 2)
                calls = []
                def fetch(title, key, year):
                    calls.append(year)
                    return self.lion(year)
                enrich(self.db, 'test-key', retry_ambiguous=True, limit=2, fetcher=fetch)
                self.assertEqual(calls, [2019, 2020])

    def test_unsuccessful_fallback_preserves_ambiguous_and_is_cached(self):
        self.prepare_remakes()
        calls = []
        def fetch(title, key, year):
            calls.append(year)
            return self.lion(1994) if year is None else {'Response': 'False', 'Error': 'Movie not found!'}
        enrich(self.db, 'test-key', limit=3, fetcher=fetch)
        self.assertEqual(calls, [None, 2019, 2020])
        enrich(self.db, 'test-key', retry_ambiguous=True,
               fetcher=lambda *_: self.fail('Repeated cached query'))
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute('SELECT match_status,imdb_id FROM dim_movie').fetchall(),
                             [('ambiguous', None), ('ambiguous', None)])

    def test_fallback_still_validates_title_and_year(self):
        self.prepare_remakes()
        def fetch(title, key, year):
            return self.lion(1994) if year is None else self.lion(year, 'Different film')
        enrich(self.db, 'test-key', limit=3, fetcher=fetch)
        with duckdb.connect(str(self.db)) as con:
            self.assertEqual(con.execute("SELECT count(*) FROM dim_movie WHERE match_status='matched'").fetchone()[0], 0)

    def test_fallback_invalid_key_stops_batch(self):
        self.prepare_remakes()
        def fetch(title, key, year):
            return self.lion(1994) if year is None else {'Response': 'False', 'Error': 'Invalid API key!'}
        result = enrich(self.db, 'test-key', limit=10, fetcher=fetch)
        self.assertEqual(result['requests'], 2)
        self.assertEqual(result['invalid_key'], 1)

    def test_legacy_cache_upgrade_reuses_title_response(self):
        self.prepare_remakes()
        enrich(self.db, 'test-key', limit=1, fetcher=lambda *_: self.lion(1994))
        with duckdb.connect(str(self.db)) as con:
            con.execute('DROP TABLE omdb_query_cache')
            con.execute('ALTER TABLE omdb_lookup DROP COLUMN query_year')
        calls = []
        def fetch(title, key, year):
            calls.append(year)
            return self.lion(year)
        enrich(self.db, 'test-key', retry_ambiguous=True, limit=2, fetcher=fetch)
        self.assertEqual(calls, [2019, 2020])


if __name__ == "__main__":
    unittest.main()
