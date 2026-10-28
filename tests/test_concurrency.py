import csv
import json
import shutil
import tempfile
import threading
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import duckdb

from pipeline.run import rebuild

from pipeline.enrich_omdb import OmdbHttpError, _save_response, enrich


def movie(title, year=2020):
    return dict(Response='True', Type='movie', Title=title, Year=str(year),
                imdbID=f'tt{year}000', Genre='Drama', Runtime='100 min')


class ConcurrencyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.db = self.root / 'test.duckdb'
        self.now = datetime(2026, 10, 19, 12, tzinfo=timezone.utc)

    def prepare(self, entries=None):
        entries = entries or [(f'Film {i}', '2020-01-01') for i in range(8)]
        source = self.root / 'source.csv'
        with source.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.writer(stream)
            writer.writerow(['id', 'date', 'title', 'revenue', 'theaters', 'distributor'])
            for i, (title, date) in enumerate(entries):
                writer.writerow([str(i), date, title, 1000 - i, '' if i == 0 else 10, 'Studio'])
        rebuild(source, self.db)

    def query(self, sql, args=None):
        with duckdb.connect(str(self.db)) as con:
            return con.execute(sql, args or []).fetchall()

    def run_enrich(self, fetcher, **kwargs):
        return enrich(self.db, 'fixture', limit=20, workers=4, clock=lambda: self.now,
                      fetcher=fetcher, **kwargs)

    def test_overlap_reverse_completion_and_single_database_writer(self):
        self.prepare()
        barrier = threading.Barrier(4)
        released = [threading.Event() for _ in range(4)]
        lock = threading.Lock()
        active = peak = 0
        completion, fetch_threads, save_threads = [], set(), set()
        caller = threading.get_ident()

        def fetch(title, *_):
            nonlocal active, peak
            index = int(title.split()[-1])
            with lock:
                active += 1
                peak = max(peak, active)
                fetch_threads.add(threading.get_ident())
            if index < 4:
                barrier.wait(timeout=10)
                if index < 3:
                    self.assertTrue(released[index + 1].wait(10))
                with lock:
                    completion.append(index)
                released[index].set()
            with lock:
                active -= 1
            return movie(title)

        def save(*args):
            save_threads.add(threading.get_ident())
            return _save_response(*args)

        with patch('pipeline.enrich_omdb._save_response', side_effect=save):
            result = self.run_enrich(fetch)
        self.assertEqual(completion, [3, 2, 1, 0])
        self.assertEqual(peak, 4)
        self.assertNotIn(caller, fetch_threads)
        self.assertEqual(save_threads, {caller})
        self.assertEqual(result['period_statuses'], {'matched': 8})
        self.assertEqual(result['request_statuses'], {'matched': 8})

    def test_run_limit_one_and_small_remaining_daily_budget(self):
        self.prepare()
        calls = []
        fetch = lambda title, *_: calls.append(title) or movie(title)
        result = enrich(self.db, 'fixture', workers=8, limit=1, fetcher=fetch,
                        clock=lambda: self.now)
        self.assertEqual(result['requests'], 1)
        result = enrich(self.db, 'fixture', workers=8, limit=20, daily_cap=3, fetcher=fetch,
                        clock=lambda: self.now)
        self.assertEqual(result['requests'], 2)
        self.assertEqual(result['requests_last_24h'], 3)
        self.assertEqual(len(calls), 3)

    def test_rolling_window_includes_boundary_and_expires_old_reservations(self):
        self.prepare()
        cutoff = (self.now - timedelta(hours=24)).replace(tzinfo=None)
        self.query("INSERT INTO omdb_request_log VALUES ('old',1,?,'reserved',NULL,'title')",
                   [cutoff - timedelta(microseconds=1)])
        self.query("INSERT INTO omdb_request_log VALUES ('boundary',1,?,'reserved',NULL,'title')", [cutoff])
        result = enrich(self.db, 'fixture', workers=8, limit=20, daily_cap=2,
                        fetcher=lambda title, *_: movie(title), clock=lambda: self.now)
        self.assertEqual((result['requests'], result['requests_last_24h']), (1, 2))
        self.now += timedelta(microseconds=1)
        result = enrich(self.db, 'fixture', workers=8, limit=20, daily_cap=2,
                        fetcher=lambda title, *_: movie(title), clock=lambda: self.now)
        self.assertEqual((result['requests'], result['requests_last_24h']), (1, 2))

    def test_fallback_priority_and_already_reserved_tail_of_budget(self):
        self.prepare()
        calls = []
        def fetch(title, key, year):
            calls.append((title, year))
            return movie(title, year or (1994 if title == 'Film 0' else 2020))
        result = enrich(self.db, 'fixture', workers=2, limit=3, fetcher=fetch)
        self.assertEqual(Counter(calls), Counter([('Film 0', None), ('Film 1', None), ('Film 0', 2020)]))
        self.assertEqual(result['requests'], 3)
        self.assertEqual(self.query("SELECT match_status FROM dim_movie WHERE source_title='Film 0'"),
                         [('matched',)])
        # With only two reservations, the already submitted second title wins
        # the last slot; the fallback remains eligible for a later retry.
        self.query("DELETE FROM omdb_query_cache")
        self.query("DELETE FROM omdb_lookup")
        self.query("UPDATE movie_metadata SET match_status='pending'")
        calls.clear()
        enrich(self.db, 'fixture', workers=2, limit=2, fetcher=fetch)
        self.assertEqual(Counter(calls), Counter([('Film 0', None), ('Film 1', None)]))
        self.assertEqual(self.query("SELECT match_status FROM dim_movie WHERE source_title='Film 0'"),
                         [('ambiguous',)])

    def test_cached_primary_gets_fallback_before_new_titles(self):
        self.prepare()
        self.query("INSERT INTO omdb_query_cache VALUES ('Film 0',0,?,?)",
                   [json.dumps(movie('Film 0', 1994)), self.now.replace(tzinfo=None)])
        calls = []
        enrich(self.db, 'fixture', workers=8, limit=1,
               fetcher=lambda title, key, year: calls.append((title, year)) or movie(title, year))
        self.assertEqual(calls, [('Film 0', 2020)])

    def test_shared_titles_are_classified_for_each_period_and_year(self):
        self.prepare([('Same', '2020-01-01'), ('Same', '2025-01-01'), ('Other', '2020-01-01')])
        barrier = threading.Barrier(2)
        calls = []
        def fetch(title, key, year):
            calls.append((title, year))
            if year is None:
                barrier.wait(timeout=10)
            return movie(title, year or 2020)
        result = self.run_enrich(fetch)
        self.assertEqual(Counter(calls), Counter([('Same', None), ('Other', None), ('Same', 2025)]))
        self.assertEqual(result['coalesced'], 1)
        self.assertEqual(self.query("SELECT release_year FROM dim_movie WHERE source_title='Same' ORDER BY run_start_date"),
                         [(2020,), (2025,)])

    def test_shared_fallbacks_and_worker_window(self):
        self.prepare([('Same', '2020-01-01'), ('Same', '2020-12-01'),
                      ('Other', '2020-01-01'), ('Third', '2020-01-01')])
        calls = []
        def fetch(title, key, year):
            calls.append((title, year))
            return movie(title, year or (1994 if title == 'Same' else 2020))
        from pipeline.enrich_omdb import RequestCoordinator
        original = RequestCoordinator.request
        def request(coordinator, *args):
            result = original(coordinator, *args)
            self.assertLessEqual(len(coordinator.active), 2)
            return result
        with patch.object(RequestCoordinator, 'request', request):
            enrich(self.db, 'fixture', limit=20, workers=2, fetcher=fetch)
        self.assertEqual(Counter(calls)[('Same', None)], 1)
        self.assertEqual(Counter(calls)[('Same', 2020)], 1)
        self.assertEqual(self.query("SELECT count(*) FROM dim_movie WHERE match_status='matched'"), [(4,)])

    def test_offline_and_both_retry_modes(self):
        self.prepare([('Same', '2020-01-01'), ('Same', '2021-01-01')])
        self.run_enrich(lambda *_: {'Response': 'False', 'Error': 'Movie not found!'})
        calls = []
        self.run_enrich(lambda title, *_: calls.append(title) or movie(title), retry_not_found=True)
        self.assertEqual(calls, ['Same', 'Same'])
        self.query("UPDATE movie_metadata SET match_status='ambiguous'")
        result = enrich(self.db, None, workers=8, retry_ambiguous=True,
                        fetcher=lambda *_: self.fail('offline network'))
        self.assertEqual(result.get('requests', 0), 0)
        self.assertEqual(result['cache_reused'], 2)
        self.query("UPDATE movie_metadata SET match_status='not_found'")
        result = enrich(self.db, None, workers=8, retry_not_found=True,
                        fetcher=lambda *_: self.fail('offline network'))
        self.assertEqual(result.get('cache_reused', 0), 0)

    def test_timeouts_payload_errors_and_http_status(self):
        self.prepare()
        def fetch(title, *_):
            i = int(title.split()[-1])
            if i == 0:
                raise TimeoutError()
            if i == 1:
                return []
            if i == 2:
                raise OmdbHttpError(503)
            return movie(title)
        result = self.run_enrich(fetch)
        self.assertEqual(result['request_statuses'], {'retry_later': 3, 'matched': 5})
        self.assertEqual(self.query("SELECT http_status FROM omdb_request_log WHERE http_status IS NOT NULL"), [(503,)])
        self.assertEqual(self.query("SELECT count(*) FROM omdb_query_cache"), [(5,)])

    def test_fatal_response_stops_submissions_and_drains_started_requests(self):
        for error in ('Invalid API key!', 'Request limit reached!'):
            with self.subTest(error=error):
                self.db = self.root / ('key.duckdb' if 'key' in error else 'quota.duckdb')
                self.prepare()
                barrier = threading.Barrier(4)
                fatal_ready = threading.Event()
                calls = []
                def fetch(title, *_):
                    calls.append(title)
                    barrier.wait(timeout=10)
                    if title == 'Film 0':
                        fatal_ready.set()
                        return {'Response': 'False', 'Error': error}
                    self.assertTrue(fatal_ready.wait(10))
                    return movie(title)
                result = self.run_enrich(fetch)
                self.assertEqual(len(calls), 4)
                self.assertEqual(result['requests'], 4)
                self.assertEqual(result['period_statuses'], {'matched': 3})
                self.assertEqual(self.query("SELECT count(*) FROM omdb_request_log WHERE request_status='reserved'"), [(0,)])

    def test_completed_fatal_later_in_order_prevents_replacement(self):
        self.prepare()
        # Gate the coordinator's next admission on the later response actually
        # being completed, without relying on thread scheduling or short sleeps.
        from pipeline.enrich_omdb import RequestCoordinator
        original = RequestCoordinator.resolve
        calls = []
        def resolve(coordinator, query, candidate):
            if candidate.title == 'Film 0':
                for other in coordinator.active.values():
                    if other.owner.title == 'Film 1':
                        other.future.result(timeout=10)
            return original(coordinator, query, candidate)
        def fetch(title, *_):
            calls.append(title)
            return {'Response': 'False', 'Error': 'Invalid API key!'} if title == 'Film 1' else movie(title)
        with patch.object(RequestCoordinator, 'resolve', resolve):
            self.run_enrich(fetch)
        self.assertLessEqual(len(calls), 4)
        self.assertNotIn('Film 4', calls)

    def test_interruption_preserves_results_reservations_and_resume(self):
        self.prepare()
        def fetch(title, *_):
            if title == 'Film 1':
                raise KeyboardInterrupt()
            return movie(title)
        with self.assertRaises(KeyboardInterrupt):
            enrich(self.db, 'fixture', workers=1, limit=8, fetcher=fetch, clock=lambda: self.now)
        self.assertEqual(self.query("SELECT m.source_title,l.request_status FROM omdb_request_log l "
                                    "JOIN movie_registry m USING (movie_key) ORDER BY m.source_title"),
                         [('Film 0', 'matched'), ('Film 1', 'reserved')])
        result = enrich(self.db, 'fixture', workers=4, limit=8, daily_cap=3,
                        fetcher=lambda title, *_: movie(title), clock=lambda: self.now)
        self.assertEqual(result['requests'], 1)
        self.assertEqual(result['requests_last_24h'], 3)

    def test_concurrent_interruption_leaves_all_unresolved_requests_charged(self):
        self.prepare()
        barrier = threading.Barrier(4)
        def fetch(title, *_):
            barrier.wait(timeout=10)
            if title == 'Film 1':
                raise KeyboardInterrupt()
            return movie(title)
        with self.assertRaises(KeyboardInterrupt):
            self.run_enrich(fetch)
        states = dict(self.query('SELECT request_status, count(*) FROM omdb_request_log GROUP BY 1'))
        self.assertEqual(states, {'matched': 1, 'reserved': 3})
        result = enrich(self.db, 'fixture', workers=4, limit=8, daily_cap=5,
                        fetcher=lambda title, *_: movie(title), clock=lambda: self.now)
        self.assertEqual((result['requests'], result['requests_last_24h']), (1, 5))

    def test_fatal_fallback_drains_other_started_titles(self):
        self.prepare()
        calls = []
        def fetch(title, key, year):
            calls.append((title, year))
            if year:
                return {'Response': 'False', 'Error': 'Invalid API key!'}
            return movie(title, 1994 if title == 'Film 0' else 2020)
        result = self.run_enrich(fetch)
        self.assertEqual(result['requests'], 5)
        self.assertEqual(result['invalid_key'], 1)
        self.assertEqual(result['period_statuses'], {'ambiguous': 1, 'matched': 3})
        self.assertNotIn(('Film 4', None), calls)

    def test_period_rollback_keeps_request_history_and_cache_for_resume(self):
        self.prepare()
        def broken_save(*args):
            _save_response(*args)
            raise RuntimeError('injected persistence failure')
        with patch('pipeline.enrich_omdb._save_response', side_effect=broken_save):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                enrich(self.db, 'fixture', workers=1, fetcher=lambda title, *_: movie(title))
        self.assertEqual(self.query('SELECT count(*) FROM omdb_lookup'), [(0,)])
        self.assertEqual(self.query("SELECT count(*) FROM dim_movie WHERE match_status='pending'"), [(8,)])
        self.assertEqual(self.query('SELECT request_status FROM omdb_request_log'), [('matched',)])
        result = enrich(self.db, None, workers=4, fetcher=lambda *_: self.fail('network'))
        self.assertEqual(result['period_statuses'], {'matched': 1})

    def test_request_completion_rollback_does_not_refund_reservation(self):
        self.prepare()
        with patch('pipeline.enrich_omdb.json.dumps', side_effect=RuntimeError('cache failure')):
            with self.assertRaisesRegex(RuntimeError, 'cache failure'):
                enrich(self.db, 'fixture', workers=1, fetcher=lambda title, *_: movie(title))
        self.assertEqual(self.query('SELECT request_status FROM omdb_request_log'), [('reserved',)])
        self.assertEqual(self.query('SELECT count(*) FROM omdb_query_cache'), [(0,)])

    def test_business_tables_equal_with_sufficient_budget(self):
        self.prepare([('Same', '2020-01-01'), ('Same', '2025-01-01'),
                      ('Missing', '2020-01-01'), ('Timeout', '2020-01-01'), ('Fine', '2020-01-01')])
        snapshot = self.root / 'snapshot.duckdb'
        shutil.copyfile(self.db, snapshot)
        def fetch(title, key, year):
            if title == 'Timeout':
                raise ConnectionError()
            if title == 'Missing':
                return {'Response': 'False', 'Error': 'Movie not found!'}
            return movie(title, year or 2020)
        snapshots = []
        for workers in (1, 4):
            if workers == 4:
                shutil.copyfile(snapshot, self.db)
            enrich(self.db, 'fixture', limit=20, workers=workers, fetcher=fetch)
            snapshots.append([
                self.query('SELECT * FROM dim_movie ORDER BY movie_key'),
                self.query('SELECT movie_key, lookup_status, candidate_imdb_id, candidate_title, '
                           'match_reason, response_json, query_year FROM omdb_lookup ORDER BY movie_key'),
                self.query('SELECT source_title, query_year, response_json FROM omdb_query_cache ORDER BY 1,2'),
                self.query('SELECT * FROM fact_daily_revenue ORDER BY source_id'),
            ])
        self.assertEqual(*snapshots)
        self.assertEqual(self.query('SELECT count(*),sum(revenue),count(*) FILTER (WHERE theaters IS NULL) '
                                    'FROM fact_daily_revenue'), [(5, 4990, 1)])

    def test_workers_validation(self):
        self.prepare()
        for workers in (0, -1, 1.5, True):
            with self.subTest(workers=workers), self.assertRaises(ValueError):
                enrich(self.db, None, workers=workers)


if __name__ == '__main__':
    unittest.main()
