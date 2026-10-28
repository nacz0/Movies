"""Exercise the real dbt executable and existing dashboard against isolated files."""

import csv
from datetime import date
import http.client
import json
from pathlib import Path
import shutil
import tempfile
import threading
import unittest
from unittest.mock import patch

import duckdb

from dashboard import queries
from dashboard.server import DashboardServer
from pipeline import ROOT
from pipeline.run import rebuild, run_dbt
from tests.test_enrich import movie

FIXTURE = ROOT / 'tests/fixtures/revenues.csv'


class DbtTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / 'warehouse.duckdb'
        self.csv = self.db.with_suffix('.csv')
        shutil.copyfile(FIXTURE, self.csv)
        guard = patch('urllib.request.urlopen', side_effect=AssertionError('Real HTTP forbidden in tests'))
        guard.start()
        self.addCleanup(guard.stop)

    def query(self, sql, params=None):
        with duckdb.connect(str(self.db)) as con:
            return con.execute(sql, params or []).fetchall()

    def write(self, rows):
        with self.csv.open('w', newline='', encoding='utf-8') as handle:
            writer = csv.writer(handle)
            writer.writerow(['id', 'date', 'title', 'revenue', 'theaters', 'distributor'])
            writer.writerows(rows)

    def test_periods_idempotency_and_all_dashboard_queries(self):
        first = rebuild(self.csv, self.db)
        self.assertEqual((first['fact_rows'], first['periods'], first['total_revenue']), (6, 3, 335))
        self.assertEqual(self.query("SELECT run_start_date, run_end_date FROM dim_movie "
                                    "WHERE source_title='Same' ORDER BY run_start_date"),
                         [(date(2020, 1, 1), date(2020, 6, 29)),
                          (date(2020, 12, 27), date(2021, 1, 1))])
        before = self.query('SELECT * FROM dim_movie ORDER BY movie_key')
        second = rebuild(self.csv, self.db)
        self.assertEqual(first, second)
        self.assertEqual(self.query('SELECT * FROM dim_movie ORDER BY movie_key'), before)
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(0,)])
        with duckdb.connect(str(self.db)) as con:
            start, end = queries.date_bounds(con)
            self.assertEqual(queries.overview(con, start, end)['revenue'], 335)
            self.assertEqual(queries.overview(con, start, end, ['Studio Y'])['revenue'], 70)
            self.assertEqual(queries.overview(con, start, end, genre='Drama')['revenue'], 0)
            self.assertEqual(queries.genre_options(con), [])
            self.assertEqual(len(queries.distributor_options(con)), 3)
            trend = queries.monthly_trend(con, start, end)
            self.assertEqual((len(trend), sum(r['revenue'] for r in trend)), (13, 335))
            self.assertIn(0, [r['revenue'] for r in trend])
            self.assertEqual(sum(r['revenue'] for r in queries.enrichment_status(con, start, end)), 335)
            sorts = [(queries.movie_ranking, ['movie', 'revenue', 'reported_days', 'genre', 'imdb_rating', 'match_status']),
                     (queries.film_ranking, ['film', 'release_year', 'revenue', 'periods', 'reported_days', 'genre', 'imdb_rating']),
                     (queries.distributor_ranking, ['distributor', 'revenue', 'movies'])]
            for function, columns in sorts:
                for sort in columns:
                    for direction in ('asc', 'desc'):
                        function(con, start, end, limit=10, sort=sort, direction=direction)
            self.assertEqual(queries.movie_ranking(con, start, end, limit=1,
                             sort='movie', direction='desc')[0]['revenue'], 90)
            with self.assertRaises(ValueError):
                queries.movie_ranking(con, start, end, sort='revenue; DROP TABLE dim_movie')
        # Exercise the real HTTP endpoint. http.client is local only, OMDb stays blocked.
        with DashboardServer(('127.0.0.1', 0), self.db) as server:
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                client = http.client.HTTPConnection('127.0.0.1', server.server_port, timeout=10)
                for path in ('/', '/api/meta', '/api/dashboard?limit=10&movie_sort=movie&movie_dir=asc',
                             '/api/dashboard?distributor=Studio%20Y'):
                    client.request('GET', path)
                    response = client.getresponse()
                    body = response.read()
                    self.assertEqual(response.status, 200, body)
                    if path == '/':
                        self.assertIn(b'release_period_ranking.csv', body)
                        self.assertIn(b'confirmed_film_ranking.csv', body)
                    if 'distributor=' in path:
                        self.assertEqual(json.loads(body)['base']['revenue'], 70)
                client.request('GET', '/api/dashboard?movie_sort=bad')
                response = client.getresponse()
                response.read()
                self.assertEqual(response.status, 400)
                client.close()
            finally:
                server.shutdown()
                thread.join(timeout=10)

    def test_changed_start_changes_key_and_rechecks_cache(self):
        self.write([['0001', '2020-01-01', 'Same', '100', '', 'Studio']])
        with patch('pipeline.enrich.fetch_title', side_effect=lambda title, *_: movie(title)) as fetch:
            rebuild(self.csv, self.db, api_key='fixture')
            first_key = self.query('SELECT movie_key FROM dim_movie')[0][0]
            self.assertEqual(fetch.call_count, 1)
        self.write([['0001', '2020-01-01', 'Same', '120', '', 'Studio'],
                    ['0002', '2019-12-31', 'Same', '5', '', 'Studio']])
        result = rebuild(self.csv, self.db)
        self.assertEqual((result['fact_rows'], result['total_revenue']), (2, 125))
        self.assertNotEqual(self.query('SELECT movie_key FROM dim_movie')[0][0], first_key)
        self.assertEqual(self.query('SELECT match_status, imdb_id FROM dim_movie'), [('ambiguous', None)])
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_cache'), [(1,)])
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(1,)])
        # Corrections/deletions replace the snapshot and remove unused dimensions.
        self.write([['0002', '2019-12-31', 'Same', '7', '', 'Studio']])
        self.assertEqual(rebuild(self.csv, self.db)['total_revenue'], 7)
        self.assertEqual(self.query('SELECT count(*) FROM dim_date'), [(1,)])

    def test_matched_periods_combine_in_film_ranking(self):
        with patch('pipeline.enrich.fetch_title', side_effect=lambda title, *_: movie(title)) as fetch:
            first = rebuild(self.csv, self.db, api_key='fixture')
            self.assertEqual(fetch.call_count, 2)
        second = rebuild(self.csv, self.db)
        self.assertEqual((first['total_revenue'], second['total_revenue']), (335, 335))
        self.assertEqual(second['enrichment']['requests'], 0)
        with duckdb.connect(str(self.db)) as con:
            start, end = queries.date_bounds(con)
            films = queries.film_ranking(con, start, end, genre='Drama')
            # The fake response deliberately has the same IMDb ID for all periods.
            self.assertEqual([(r['revenue'], r['periods']) for r in films], [(335, 3)])
            self.assertEqual(queries.overview(con, start, end)['matched_revenue'], 335)
            self.assertEqual(queries.genre_options(con), ['Drama'])

    def test_dbt_tests_detect_corruption_and_stop_pipeline(self):
        rebuild(self.csv, self.db)
        self.query("UPDATE fact_daily_revenue SET revenue=-1, theaters=0, movie_key='missing' WHERE source_id='a'")
        with self.assertRaisesRegex(RuntimeError, 'dbt test') as failure:
            run_dbt(self.db, 'test', '--select', 'test_type:data')
        self.assertIn('reconcile_rows', str(failure.exception))
        self.assertIn('reconcile_totals', str(failure.exception))
        self.assertIn('relationships_fact_daily_revenue_movie_key', str(failure.exception))
        self.assertIn('valid_values', str(failure.exception))
        with patch('pipeline.enrich.fetch_title', side_effect=lambda title, *_: movie(title)):
            from pipeline.run import run_dbt as real_dbt
            def fail_build(path, *arguments):
                if arguments[0] == 'build':
                    raise RuntimeError('dbt quality test failed')
                real_dbt(path, *arguments)
            with patch('pipeline.run.run_dbt', side_effect=fail_build):
                with self.assertRaisesRegex(RuntimeError, 'quality test failed'):
                    rebuild(None, self.db, api_key='fixture', limit=1)
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_cache'), [(1,)])
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(1,)])
        # A normal rerun repairs marts and retains consumed budget/cache.
        rebuild(None, self.db)
        self.assertEqual(self.query('SELECT sum(revenue) FROM fact_daily_revenue'), [(335,)])
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(1,)])
