"""Bounded HTTP batches; only the calling thread reads or writes DuckDB."""

from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from uuid import uuid4

import duckdb

from pipeline import ROOT

FATAL = {'invalid_key', 'quota_exhausted'}


@dataclass(frozen=True)
class Period:
    movie_key: str
    source_title: str
    run_start_date: date
    run_end_date: date
    revenue: int


def load_api_key() -> str:
    key = os.environ.get('OMDB_API_KEY', '').strip()
    if not key and (ROOT / '.env').is_file():
        for line in (ROOT / '.env').read_text(encoding='utf-8-sig').splitlines():
            name, sep, value = line.partition('=')
            if sep and name.strip() == 'OMDB_API_KEY':
                key = value.strip().strip('\"\'')
    if not key or key == 'replace_with_your_free_key':
        raise ValueError('Set OMDB_API_KEY in the environment or .env')
    return key


def query_title(title: str) -> str:
    """This exact normalized text is sent to OMDb and used as the cache identity."""
    return ' '.join(unicodedata.normalize('NFKC', title).casefold().split())


def _title(title: str) -> str:
    text = unicodedata.normalize('NFKD', title.casefold())
    return ' '.join(re.findall(r'\w+', ''.join(c for c in text if not unicodedata.combining(c))))


def classify(payload: dict, title: str, first_year: int, last_year: int) -> tuple[str, str]:
    if payload.get('Response') != 'True':
        error = str(payload.get('Error', '')).lower()
        if 'limit' in error:
            return 'quota_exhausted', 'OMDb request limit'
        if 'api key' in error or 'invalid key' in error:
            return 'invalid_key', 'OMDb rejected the key'
        if 'not found' in error:
            return 'not_found', 'Title not found'
        return 'retry_later', 'Malformed response or temporary API error'
    if payload.get('Type') != 'movie' or not re.fullmatch(r'tt\d+', str(payload.get('imdbID', ''))):
        return 'ambiguous', 'Result is not a movie with an IMDb ID'
    if 'short' in {g.strip().casefold() for g in str(payload.get('Genre', '')).split(',')}:
        return 'ambiguous', 'Short film requires manual review'
    if _title(str(payload.get('Title', ''))) != _title(title):
        return 'ambiguous', 'Different title'
    year = str(payload.get('Year', ''))
    if not re.fullmatch(r'\d{4}', year):
        return 'ambiguous', 'Missing release year'
    if not 0 <= first_year - int(year) <= 2 or last_year - first_year > 3:
        return 'ambiguous', 'Release year needs review'
    return 'matched', 'Title and year agree'


def fetch_title(title: str, api_key: str, year: int | None = None) -> dict:
    params = dict(apikey=api_key, t=title, type='movie')
    if year is not None:
        params['y'] = str(year)
    request = urllib.request.Request('https://www.omdbapi.com/?' + urllib.parse.urlencode(params),
                                    headers={'User-Agent': 'Movies-pipeline/2'})
    with urllib.request.urlopen(request, timeout=15) as response:
        body = response.read(1_000_001)
        if len(body) > 1_000_000:
            raise ValueError('Oversized OMDb response')
        return json.loads(body)


def _fetch(fetcher, title, key, year):
    """Do not expose exception text: HTTP URLs may contain the API key."""
    try:
        payload = fetcher(title, key, year or None)
        if not isinstance(payload, dict):
            raise ValueError('Expected JSON object')
        status = ('response' if payload.get('Response') == 'True' else
                  classify(payload, title, 2000, 2000)[0])
        return payload, status
    except urllib.error.HTTPError as exc:
        status = 'invalid_key' if exc.code in (401, 403) else (
            'quota_exhausted' if exc.code == 429 else 'retry_later')
        return None, status
    except (OSError, ValueError):
        return None, 'retry_later'


def enrich(db_path: Path, api_key: str | None = None, *, workers: int = 4, limit: int = 100,
           daily_cap: int = 900, fetcher=None, clock=None) -> dict:
    if not 1 <= workers <= 32 or limit < 0 or not 0 <= daily_cap <= 900:
        raise ValueError('Use 1..32 workers, a nonnegative limit and a daily cap of 0..900')
    fetcher = fetcher or fetch_title
    clock = clock or (lambda: datetime.now(timezone.utc))
    counts = Counter(requests=0, cache_reused=0)
    stopped = False
    with duckdb.connect(str(db_path)) as con, ThreadPoolExecutor(max_workers=workers) as pool:
        cache = {(t, y): json.loads(p) for t, y, p in con.execute(
            'SELECT query_title, query_year, response_json FROM movies_state.omdb_cache').fetchall()}
        groups = {}
        for row in con.execute('SELECT * FROM main.int_omdb_candidates ORDER BY revenue DESC, movie_key').fetchall():
            period = Period(*row)
            groups.setdefault(query_title(period.source_title), []).append(period)

        def requests(identities):
            nonlocal stopped
            results, futures = {}, {}
            # At most workers unique requests are in flight. Reservations autocommit
            # BEFORE submit, and even interrupted requests continue to consume budget.
            for title, year in dict.fromkeys(identities):
                identity = (title, year)
                if identity in cache:
                    results[identity] = (cache[identity], 'cached')
                    counts['cache_reused'] += 1
                    continue
                now = clock()
                if now.tzinfo is None:
                    raise ValueError('clock must return a timezone-aware datetime')
                used = con.execute('SELECT count(*) FROM movies_state.omdb_requests '
                                   'WHERE requested_at >= ?', [now - timedelta(hours=24)]).fetchone()[0]
                if not api_key or stopped or counts['requests'] >= limit or used >= daily_cap:
                    continue
                request_id = str(uuid4())
                con.execute('INSERT INTO movies_state.omdb_requests VALUES (?, ?, ?, ?, ?)',
                            [request_id, now, title, year, 'reserved'])
                counts['requests'] += 1
                futures[pool.submit(_fetch, fetcher, title, api_key, year)] = (identity, request_id)
            for future in as_completed(futures):
                identity, request_id = futures[future]
                payload, status = future.result()
                results[identity] = (payload, status)
                stopped |= status in FATAL
                con.execute('UPDATE movies_state.omdb_requests SET outcome=? WHERE request_id=?',
                            [status, request_id])
                if payload is not None and status in {'response', 'not_found'}:
                    con.execute('INSERT OR REPLACE INTO movies_state.omdb_cache VALUES (?, ?, ?, ?)',
                                [*identity, json.dumps(payload, ensure_ascii=False), clock()])
                    cache[identity] = payload
            return results

        matches = []
        titles = list(groups)
        for offset in range(0, len(titles), workers):
            batch = titles[offset:offset + workers]
            primary = requests((t, 0) for t in batch)
            pending = []
            for title in batch:
                payload, status = primary.get((title, 0), (None, 'pending'))
                for period in groups[title]:
                    state, reason = (classify(payload, period.source_title, period.run_start_date.year,
                                              period.run_end_date.year) if payload else
                                     (status, 'No cached response or request failed'))
                    pending.append((period, title, payload, state, reason))
            # Unique year fallbacks, still in bounded batches; they have the same budget.
            fallback_ids = list(dict.fromkeys((title, period.run_start_date.year)
                for period, title, _, status, reason in pending
                if reason == 'Release year needs review' or
                   (status != 'matched' and (title, period.run_start_date.year) in cache)))
            fallbacks = {}
            for i in range(0, len(fallback_ids), workers):
                fallbacks.update(requests(fallback_ids[i:i + workers]))
            for period, title, payload, status, reason in pending:
                if status != 'matched':
                    alternative, _ = fallbacks.get((title, period.run_start_date.year), (None, None))
                    if alternative:
                        alt_status, alt_reason = classify(alternative, period.source_title,
                            period.run_start_date.year, period.run_end_date.year)
                        if alt_status == 'matched':
                            payload, status, reason = alternative, alt_status, alt_reason
                if status in FATAL:
                    status = 'retry_later'
                matches.append((period.movie_key, period.run_end_date, status, reason,
                                json.dumps(payload) if payload else None))
                counts[status] += 1
        con.execute('BEGIN')
        try:
            con.execute('DELETE FROM movies_state.omdb_matches')
            if matches:
                con.executemany('INSERT INTO movies_state.omdb_matches VALUES (?, ?, ?, ?, ?)', matches)
            con.execute('COMMIT')
        except BaseException:
            con.execute('ROLLBACK')
            raise
        counts['requests_last_24h'] = con.execute('SELECT count(*) FROM movies_state.omdb_requests '
            'WHERE requested_at >= ?', [clock() - timedelta(hours=24)]).fetchone()[0]
    return dict(counts)


def reuse_cache(old_path: Path, new_path: Path) -> dict:
    """Optional one-time copy from main/dbt format. Never modify the old file."""
    if old_path.resolve() == new_path.resolve():
        raise ValueError('Cache source and destination must be different files')
    with duckdb.connect(str(old_path.resolve()), read_only=True) as old:
        cache = old.execute('SELECT source_title, query_year, response_json, fetched_at_utc '
                            'FROM main.omdb_query_cache ORDER BY fetched_at_utc DESC').fetchall()
        reservations = old.execute('SELECT request_id, requested_at_utc FROM main.omdb_request_log').fetchall()
    with duckdb.connect(str(new_path)) as con:
        con.execute('BEGIN')
        try:
            for title, year, response, fetched in cache:
                payload = json.loads(response)
                if isinstance(payload, dict) and classify(payload, title, 2000, 2000)[0] in {
                        'matched', 'ambiguous', 'not_found'}:
                    con.execute('INSERT INTO movies_state.omdb_cache VALUES (?, ?, ?, ?) '
                                'ON CONFLICT DO NOTHING',
                                [query_title(title), year, response, fetched.replace(tzinfo=timezone.utc)])
            for identity, requested in reservations:
                con.execute('INSERT INTO movies_state.omdb_requests VALUES (?, ?, ?, 0, ?) '
                            'ON CONFLICT DO NOTHING',
                            [identity, requested.replace(tzinfo=timezone.utc), '', 'imported'])
            con.execute('COMMIT')
        except BaseException:
            con.execute('ROLLBACK')
            raise
    return {'legacy_cache_rows': len(cache), 'legacy_reservations': len(reservations)}
