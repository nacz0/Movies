"""Explicit live OMDb integration check: at most four requests per rolling 24 h.

Uses a separate local test database, retaining reservations across invocations.
Never runs as part of the network-free benchmarks or unit tests.
"""

import csv
import json
import tempfile
import threading
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from pipeline.enrich_omdb import MAX_DAILY_CAP, enrich, fetch_title, load_api_key
from pipeline.load_revenues import ROOT, load

RESULTS = ROOT / 'benchmarks/results'
LIVE_DB = RESULTS / 'live_omdb.duckdb'
PRODUCTION_DB = ROOT / 'data/box_office.duckdb'
TOTAL_BUDGET = 4
WORKERS = 3


def recent_requests(path):
    if not path.is_file():
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).replace(tzinfo=None)
    with duckdb.connect(str(path), read_only=True) as con:
        return con.execute('SELECT count(*) FROM omdb_request_log WHERE requested_at_utc >= ?',
                           [cutoff]).fetchone()[0]


def movies(con):
    return con.execute('SELECT source_title, run_start_date, match_status, imdb_id, release_year '
                       'FROM dim_movie ORDER BY source_title, run_start_date').fetchall()


def main():
    key = load_api_key()
    production_used = recent_requests(PRODUCTION_DB)
    test_used = recent_requests(LIVE_DB)
    daily_cap = min(TOTAL_BUDGET, MAX_DAILY_CAP - production_used)
    available = daily_cap - test_used
    if available <= 0:
        raise SystemExit('No remaining shared live-test budget; no requests sent')
    RESULTS.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='movies-live-') as directory:
        source = Path(directory) / 'source.csv'
        with source.open('w', newline='', encoding='utf-8') as stream:
            writer = csv.writer(stream)
            writer.writerow(['id', 'date', 'title', 'revenue', 'theaters', 'distributor'])
            writer.writerows([
                ['1', '1999-03-31', 'The Matrix', 1000, 10, 'Test studio'],
                ['2', '2000-01-01', 'The Matrix', 900, '', 'Test studio'],
                ['3', '2010-07-16', 'Inception', 800, 8, 'Test studio'],
                ['4', '2019-07-19', 'The Lion King', 700, 7, 'Test studio'],
            ])
        load(source, LIVE_DB)
    with duckdb.connect(str(LIVE_DB)) as con:
        # Keep request history durable, while forcing this invocation online.
        con.execute('DELETE FROM omdb_lookup')
        con.execute('DELETE FROM omdb_query_cache')
        con.execute("UPDATE dim_movie SET match_status='pending'")
    calls, lock = [], threading.Lock()
    active = peak = 0

    def fetch(title, api_key, year):
        nonlocal active, peak
        with lock:
            calls.append((title, year))
            active += 1
            peak = max(peak, active)
        try:
            return fetch_title(title, api_key, year)
        finally:
            with lock:
                active -= 1

    online = enrich(LIVE_DB, key, limit=available, daily_cap=daily_cap,
                    workers=WORKERS, fetcher=fetch)
    with duckdb.connect(str(LIVE_DB)) as con:
        before = movies(con)
        facts = con.execute('SELECT count(*),sum(revenue),count(*) FILTER (WHERE theaters IS NULL) '
                            'FROM fact_daily_revenue').fetchone()
        con.execute("UPDATE dim_movie SET match_status='pending'")

    def offline_fetch(*_):
        raise AssertionError('Offline mode attempted a network request')

    offline = enrich(LIVE_DB, None, limit=TOTAL_BUDGET, daily_cap=daily_cap,
                     workers=WORKERS, fetcher=offline_fetch)
    with duckdb.connect(str(LIVE_DB)) as con:
        after = movies(con)
    checks = {
        'all_periods_matched': online['period_statuses'] == {'matched': 4},
        'release_years': all(row[4] == {'The Matrix': 1999, 'Inception': 2010,
                                      'The Lion King': 2019}[row[0]] for row in before),
        'shared_title_fetched_once': Counter(calls)[('The Matrix', None)] == 1,
        'concurrent_http': 2 <= peak <= WORKERS,
        'shared_budget': len(calls) + test_used <= TOTAL_BUDGET,
        'local_daily_cap': len(calls) + test_used + production_used <= MAX_DAILY_CAP,
        'offline_zero_requests': offline.get('requests', 0) == 0,
        'offline_same_movies': before == after,
        'fact_reconciliation': facts == (4, 3400, 1),
    }
    summary = dict(checked_at_utc=datetime.now(timezone.utc).isoformat(),
                   passed=all(checks.values()), shared_budget=TOTAL_BUDGET,
                   workers=WORKERS, production_requests_before=production_used,
                   test_requests_before=test_used, actual_calls=len(calls),
                   peak_concurrent_calls=peak, year_fallback_calls=sum(y is not None for _, y in calls),
                   online=online, offline=offline, checks=checks,
                   movies=[dict(title=r[0], start=str(r[1]), status=r[2], imdb_id=r[3], year=r[4])
                           for r in after])
    (RESULTS / 'live_omdb.json').write_text(json.dumps(summary, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(summary, indent=2))
    if not summary['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
