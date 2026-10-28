from collections import Counter
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import tempfile
import threading
import unittest
from unittest.mock import patch
import urllib.error
from urllib.parse import parse_qs, urlsplit

import duckdb

from pipeline.enrich import classify, enrich, fetch_title, query_title, reuse_cache
from pipeline.ingest import initialize


def movie(title, year=2020, **extra):
    return dict(Response='True', Type='movie', Title=title, Year=str(year),
                imdbID='tt1234567', Genre='Drama', imdbRating='7.2', Runtime='100 min', **extra)


class EnrichmentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.db = Path(temporary.name) / 'test.duckdb'
        self.now = datetime(2026, 10, 28, 12, tzinfo=timezone.utc)
        initialize(self.db)
        self.query('CREATE TABLE int_omdb_candidates (movie_key VARCHAR, source_title VARCHAR, '
                   'run_start_date DATE, run_end_date DATE, revenue BIGINT)')
        self.candidates([(f'Film {i}', '2020-01-01', '2020-01-01') for i in range(8)])

    def query(self, sql, params=None):
        with duckdb.connect(str(self.db)) as con:
            return con.execute(sql, params or []).fetchall()

    def candidates(self, rows):
        self.query('DELETE FROM int_omdb_candidates')
        for i, (title, start, end) in enumerate(rows):
            self.query('INSERT INTO int_omdb_candidates VALUES (?, ?, ?, ?, ?)',
                       [str(i), title, start, end, 1000 - i])

    def run_enrich(self, fetcher, **options):
        return enrich(self.db, 'fake-key', fetcher=fetcher, clock=lambda: self.now, **options)

    def test_classification_is_conservative(self):
        cases = [
            (movie('Film'), 'Film', 2020, 2020, 'matched'),
            (movie('Film'), 'Film', 2022, 2022, 'matched'),
            (movie('Film'), 'Film', 2019, 2020, 'ambiguous'),
            (movie('Film'), 'Film', 2023, 2023, 'ambiguous'),
            (movie('Film'), 'Film', 2020, 2024, 'ambiguous'),
            (movie('Film: subtitle'), 'Film', 2020, 2020, 'ambiguous'),
            (movie('Film') | {'Genre': 'Drama, Short'}, 'Film', 2020, 2020, 'ambiguous'),
            (movie('Film') | {'Type': 'series'}, 'Film', 2020, 2020, 'ambiguous'),
            (movie('Film') | {'Year': 'N/A'}, 'Film', 2020, 2020, 'ambiguous'),
            (movie('Film') | {'imdbID': 'bad'}, 'Film', 2020, 2020, 'ambiguous'),
            ({'Response': 'False', 'Error': 'Movie not found!'}, 'Film', 2020, 2020, 'not_found'),
        ]
        for payload, title, start, end, status in cases:
            with self.subTest(payload=payload, start=start, end=end):
                self.assertEqual(classify(payload, title, start, end)[0], status)

    def test_http_concurrency_is_bounded_and_reserved_before_dispatch(self):
        lock = threading.Lock()
        barrier = threading.Barrier(4)
        active = peak = 0
        caller = threading.get_ident()
        threads = set()

        def fetch(title, *_):
            nonlocal active, peak
            # This is a separate worker-local connection used only by the test.
            self.assertTrue(self.query('SELECT request_id FROM movies_state.omdb_requests '
                                       'WHERE query_title=? AND outcome=?', [title, 'reserved']))
            with lock:
                threads.add(threading.get_ident())
                active += 1
                peak = max(peak, active)
            barrier.wait(timeout=10)
            with lock:
                active -= 1
            return movie(title)

        self.assertEqual(self.run_enrich(fetch, workers=4)['requests'], 8)
        self.assertEqual(peak, 4)
        self.assertNotIn(caller, threads)
        self.assertEqual(self.query('SELECT DISTINCT outcome FROM movies_state.omdb_requests'), [('response',)])
        self.assertEqual(self.query('SELECT DISTINCT match_status FROM movies_state.omdb_matches'), [('matched',)])

    def test_duplicate_titles_and_year_fallbacks_share_requests(self):
        self.candidates([('Same', '2020-01-01', '2020-01-01'),
                         (' same ', '2020-12-01', '2020-12-01'),
                         ('Same', '2025-01-01', '2025-01-01')])
        calls = []
        def fetch(title, key, year):
            calls.append((title, year))
            return movie(title, year or 1994)
        first = self.run_enrich(fetch)
        self.assertEqual(Counter(calls), Counter([('same', None), ('same', 2020), ('same', 2025)]))
        self.assertEqual(first['matched'], 3)
        before = self.query('SELECT * FROM movies_state.omdb_matches ORDER BY movie_key')
        second = self.run_enrich(lambda *_: self.fail('cache should avoid HTTP'))
        self.assertEqual(second['requests'], 0)
        offline = enrich(self.db, None, fetcher=lambda *_: self.fail('offline HTTP'))
        self.assertEqual(offline['requests'], 0)
        self.assertEqual(self.query('SELECT * FROM movies_state.omdb_matches ORDER BY movie_key'), before)

    def test_punctuation_is_part_of_request_cache_identity(self):
        self.assertEqual(query_title('  FILM   Title '), 'film title')
        self.assertNotEqual(query_title('Film: Title'), query_title('Film Title'))
        self.assertNotEqual(query_title('Café'), query_title('Cafe'))

    def test_offline_reuses_year_specific_cache_without_title_only_response(self):
        self.query('INSERT INTO movies_state.omdb_cache VALUES (?, ?, ?, ?)',
                   ['film 0', 2020, json.dumps(movie('Film 0')), self.now])
        result = enrich(self.db, None, fetcher=lambda *_: self.fail('offline HTTP'))
        self.assertEqual((result['requests'], result['matched'], result['cache_reused']), (0, 1, 1))

    def test_run_limit_and_rolling_inclusive_24_hour_budget(self):
        cutoff = self.now - timedelta(hours=24)
        self.query('INSERT INTO movies_state.omdb_requests VALUES (?, ?, ?, 0, ?)',
                   ['expired', cutoff - timedelta(microseconds=1), 'old', 'reserved'])
        self.query('INSERT INTO movies_state.omdb_requests VALUES (?, ?, ?, 0, ?)',
                   ['boundary', cutoff, 'old', 'reserved'])
        fetch = lambda title, *_: movie(title)
        first = self.run_enrich(fetch, limit=20, daily_cap=2)
        self.assertEqual((first['requests'], first['requests_last_24h']), (1, 2))
        self.now += timedelta(microseconds=1)
        second = self.run_enrich(fetch, limit=1, daily_cap=2)
        self.assertEqual((second['requests'], second['requests_last_24h']), (1, 2))
        third = self.run_enrich(fetch, limit=10, daily_cap=2)
        self.assertEqual(third['requests'], 0)

    def test_fallback_consumes_same_request_budget(self):
        self.candidates([('Same', '2020-01-01', '2020-01-01')])
        calls = []
        def fetch(title, key, year):
            calls.append(year)
            return movie(title, year or 1994)
        self.assertEqual(self.run_enrich(fetch, limit=1)['ambiguous'], 1)
        self.assertEqual(calls, [None])
        self.assertEqual(self.run_enrich(fetch, limit=1)['matched'], 1)
        self.assertEqual(calls, [None, 2020])

    def test_cache_is_reclassified_when_period_moves_or_extends(self):
        self.candidates([('Same', '2020-01-01', '2020-01-01')])
        self.run_enrich(lambda title, *_: movie(title))
        for start, end in [('2019-12-31', '2020-01-01'), ('2020-01-01', '2024-01-01')]:
            self.candidates([('Same', start, end)])
            result = enrich(self.db, None, fetcher=lambda *_: self.fail('offline HTTP'))
            self.assertEqual(result['ambiguous'], 1)
            self.assertEqual(self.query('SELECT match_status FROM movies_state.omdb_matches'), [('ambiguous',)])

    def test_fatal_errors_stop_new_batches(self):
        for payload, status in [({'Response': 'False', 'Error': 'Invalid API key!'}, 'invalid_key'),
                                ({'Response': 'False', 'Error': 'Request limit reached!'}, 'quota_exhausted')]:
            with self.subTest(status=status):
                result = self.run_enrich(lambda *_: payload, workers=2)
                self.assertEqual(result['requests'], 2)
                self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_cache'), [(0,)])
                self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests '
                                           'WHERE outcome=?', [status]), [(2,)])

    def test_failures_are_retryable_and_consume_budget(self):
        def fetch(title, *_):
            i = int(title.split()[-1])
            if i == 0:
                raise TimeoutError()
            if i == 1:
                return []
            if i == 2:
                return {}
            if i == 3:
                raise urllib.error.HTTPError('https://example.invalid/?apikey=secret', 503, '', {}, None)
            return movie(title)
        first = self.run_enrich(fetch)
        self.assertEqual((first['requests'], first['retry_later'], first['matched']), (8, 4, 4))
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_cache'), [(4,)])
        second = self.run_enrich(lambda title, *_: movie(title))
        self.assertEqual((second['requests'], second['matched']), (4, 8))
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(12,)])

    def test_interruption_preserves_reservation_and_previously_saved_cache(self):
        self.run_enrich(lambda title, *_: movie(title), limit=1, workers=1)
        before = self.query('SELECT * FROM movies_state.omdb_cache')
        def interrupted(*_):
            raise KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.run_enrich(interrupted, workers=1)
        self.assertEqual(self.query('SELECT * FROM movies_state.omdb_cache'), before)
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(2,)])
        self.assertEqual(self.query("SELECT count(*) FROM movies_state.omdb_requests WHERE outcome='reserved'"), [(1,)])
        result = self.run_enrich(lambda *_: self.fail('budget exhausted'), daily_cap=2)
        self.assertEqual(result['requests'], 0)

    def test_invalid_options_and_zero_budget(self):
        for options in [dict(workers=0), dict(workers=33), dict(limit=-1), dict(daily_cap=901)]:
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.run_enrich(lambda *_: self.fail('HTTP'), **options)
        self.assertEqual(self.run_enrich(lambda *_: self.fail('HTTP'), limit=0)['requests'], 0)

    def test_http_request_encoding_timeout_and_optional_year(self):
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, _): return json.dumps(movie('Film')).encode()
        with patch('urllib.request.urlopen', return_value=Response()) as opened:
            fetch_title('Film & Title', 'fake-key', 2020)
            request = opened.call_args.args[0]
            self.assertEqual(parse_qs(urlsplit(request.full_url).query)['y'], ['2020'])
            self.assertEqual(parse_qs(urlsplit(request.full_url).query)['t'], ['Film & Title'])
            self.assertEqual(opened.call_args.kwargs['timeout'], 15)
            fetch_title('Film', 'fake-key')
            self.assertNotIn('y', parse_qs(urlsplit(opened.call_args.args[0].full_url).query))

    def test_http_auth_quota_errors_and_invalid_json(self):
        for code, status in [(401, 'invalid_key'), (403, 'invalid_key'), (429, 'quota_exhausted')]:
            def fetch(*_, code=code):
                raise urllib.error.HTTPError('https://example.invalid/?apikey=secret', code, '', {}, None)
            with self.subTest(code=code):
                result = self.run_enrich(fetch, workers=1)
                self.assertEqual(result['requests'], 1)
                self.assertEqual(result['retry_later'], 1)
                self.assertEqual(self.query('SELECT outcome FROM movies_state.omdb_requests '
                                           'ORDER BY rowid DESC LIMIT 1'), [(status,)])
        class Response:
            def __enter__(self): return self
            def __exit__(self, *_): pass
            def read(self, _): return b'not JSON'
        with patch('urllib.request.urlopen', return_value=Response()):
            result = self.run_enrich(fetch_title, workers=1, limit=1)
        self.assertEqual((result['requests'], result['retry_later']), (1, 1))
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_cache'), [(0,)])

    def test_optional_legacy_copy_is_read_only_and_idempotent(self):
        legacy = self.db.parent / 'legacy.duckdb'
        with duckdb.connect(str(legacy)) as con:
            con.execute('CREATE TABLE omdb_query_cache (source_title VARCHAR, query_year INTEGER, '
                        'response_json VARCHAR, fetched_at_utc TIMESTAMP)')
            con.execute('INSERT INTO omdb_query_cache VALUES (?, 0, ?, ?)',
                        ['Film 0', json.dumps(movie('Film 0')), self.now])
            con.execute('CREATE TABLE omdb_request_log (request_id VARCHAR, requested_at_utc TIMESTAMP)')
            con.execute('INSERT INTO omdb_request_log VALUES (?, ?)', ['legacy-request', self.now])
        before = legacy.read_bytes()
        for _ in range(2):
            reuse_cache(legacy, self.db)
        self.assertEqual(legacy.read_bytes(), before)
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_requests'), [(1,)])
        self.assertEqual(self.query('SELECT count(*) FROM movies_state.omdb_cache'), [(1,)])
        self.assertEqual(enrich(self.db)['matched'], 1)
