"""Enrich high-revenue reporting periods, reusing saved OMDb responses."""

from __future__ import annotations

import json
import os
import re
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import duckdb

from pipeline import ROOT

MAX_DAILY_CAP = 900
FATAL_STATUSES = {'invalid_key', 'quota_exhausted'}
CACHEABLE_STATUSES = {'matched', 'ambiguous', 'not_found'}


class OmdbHttpError(ConnectionError):
    def __init__(self, status: int):
        super().__init__(f"OMDb HTTP status {status}")
        self.status = status


def load_api_key() -> str:
    key = os.environ.get("OMDB_API_KEY", "").strip()
    if not key and (ROOT / ".env").is_file():
        for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
            name, sep, value = line.partition("=")
            if sep and name.strip() == "OMDB_API_KEY":
                key = value.strip().strip('"\'')
                break
    if not key:
        raise ValueError("Set OMDB_API_KEY in the environment or project .env file")
    return key


def fetch_title(title: str, api_key: str, year: int | None = None) -> dict:
    params = {"apikey": api_key, "t": title, "type": "movie"}
    if year is not None:
        params["y"] = str(year)
    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"https://www.omdbapi.com/?{query}",
        headers={"User-Agent": "Movies-data-pipeline/1.0"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            body = response.read(1_000_001)
            if len(body) > 1_000_000:
                raise ValueError("OMDb response is unexpectedly large")
            return json.loads(body)
    except urllib.error.HTTPError as exc:
        # Do not print the exception: its URL contains the key.
        raise OmdbHttpError(exc.code) from None
    except (urllib.error.URLError, TimeoutError):
        raise ConnectionError("OMDb request failed") from None


def _title(value: str) -> str:
    value = unicodedata.normalize("NFKD", value.casefold())
    value = "".join(char for char in value if not unicodedata.combining(char))
    return " ".join(re.findall(r"\w+", value))


def classify(payload: dict, source_title: str, first_year: int, last_year: int) -> tuple[str, str]:
    if payload.get("Response") != "True":
        error = str(payload.get("Error", "")).lower()
        if "limit" in error:
            return "quota_exhausted", "OMDb request limit"
        if "api key" in error or "invalid key" in error:
            return "invalid_key", "OMDb rejected the key"
        if "not found" in error:
            return "not_found", "Title not found"
        return "retry_later", "OMDb error"
    if payload.get("Type") != "movie" or not re.fullmatch(r"tt\d+", str(payload.get("imdbID", ""))):
        return "ambiguous", "Result is not a movie with an IMDb ID"
    if 'short' in {genre.strip().casefold() for genre in str(payload.get('Genre', '')).split(',')}:
        return "ambiguous", "Short film requires manual review"
    if _title(str(payload.get("Title", ""))) != _title(source_title):
        return "ambiguous", "Different title"
    year = str(payload.get("Year", ""))
    if not re.fullmatch(r"\d{4}", year):
        return "ambiguous", "Missing release year"
    if not 0 <= first_year - int(year) <= 2 or last_year - first_year > 3:
        return "ambiguous", "Release year needs review"
    return "matched", "Title and year agree"


def _number(value: object, pattern: str, cast):
    match = re.fullmatch(pattern, str(value or ""))
    return cast(match.group(1)) if match else None


def _save_response(con, movie_key: int, payload: dict, status: str, reason: str,
                   fetched_at: datetime, query_year: int | None = None) -> None:
    con.execute("""
        INSERT INTO omdb_lookup
            (movie_key, lookup_status, candidate_imdb_id, candidate_title,
             match_reason, response_json, fetched_at_utc, query_year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (movie_key) DO UPDATE SET
            lookup_status = excluded.lookup_status,
            candidate_imdb_id = excluded.candidate_imdb_id,
            candidate_title = excluded.candidate_title,
            match_reason = excluded.match_reason,
            response_json = excluded.response_json,
            fetched_at_utc = excluded.fetched_at_utc,
            query_year = excluded.query_year
    """, [movie_key, status, payload.get("imdbID"), payload.get("Title"),
          reason, json.dumps(payload, ensure_ascii=False), fetched_at, query_year])
    if status == "matched":
        genre = str(payload.get("Genre", "")).split(",", 1)[0].strip()
        con.execute("""
            UPDATE movie_metadata SET match_status = 'matched', imdb_id = ?,
                omdb_title = ?, release_year = ?, primary_genre = ?,
                imdb_rating = ?, runtime_minutes = ? WHERE movie_key = ?
        """, [payload["imdbID"], payload["Title"], int(payload["Year"]),
              genre if genre and genre != "N/A" else None,
              _number(payload.get("imdbRating"), r"(\d+(?:\.\d+)?)", float),
              _number(payload.get("Runtime"), r"(\d+) min", int), movie_key])
    else:
        con.execute("""UPDATE movie_metadata SET match_status = ?, imdb_id = NULL,
                    omdb_title = NULL, release_year = NULL, primary_genre = NULL,
                    imdb_rating = NULL, runtime_minutes = NULL WHERE movie_key = ?""",
                    [status, movie_key])


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class Candidate:
    movie_key: int
    title: str
    first_year: int
    last_year: int


@dataclass
class FetchResult:
    payload: dict | None
    error_status: str | None = None
    http_status: int | None = None


@dataclass
class Query:
    owner: Candidate
    year: int | None
    fetched_at: datetime
    future: Future | None = None
    request_id: str | None = None
    result: FetchResult | None = None


def _fetch(fetcher, title, api_key, year) -> FetchResult:
    """Worker boundary: no database access, classification or budget decisions."""
    try:
        payload = fetcher(title, api_key, year)
        if not isinstance(payload, dict):
            raise ValueError("OMDb returned an invalid response")
        return FetchResult(payload)
    except OmdbHttpError as exc:
        return FetchResult(None, 'invalid_key' if exc.status in (401, 403) else 'retry_later', exc.status)
    except (ConnectionError, TimeoutError, ValueError):
        return FetchResult(None, 'retry_later')


def _classification(result: FetchResult, candidate: Candidate) -> tuple[str, str]:
    if result.error_status:
        return result.error_status, ('OMDb rejected the key' if result.error_status == 'invalid_key'
                                     else 'OMDb request failed')
    return classify(result.payload, candidate.title, candidate.first_year, candidate.last_year)


def _candidates(con, retry_not_found, retry_ambiguous):
    rows = con.execute("""
        SELECT m.movie_key, m.source_title, m.first_year, m.last_year, m.revenue
        FROM intermediate.int_omdb_candidates m
        WHERE (? AND m.match_status = 'not_found')
           OR (? AND m.match_status = 'ambiguous')
           OR (NOT ? AND NOT ? AND m.match_status IN ('pending', 'retry_later'))
        ORDER BY 5 DESC, 2, 1
    """, [retry_not_found, retry_ambiguous, retry_not_found, retry_ambiguous]).fetchall()
    return [Candidate(*row[:4]) for row in rows]


def _upgrade_cache(con):
    cached_keys = set(con.execute('SELECT source_title, query_year FROM omdb_query_cache').fetchall())
    for title, year, response, fetched_at in con.execute("""
        SELECT m.source_title, coalesce(l.query_year, 0), l.response_json, l.fetched_at_utc
        FROM omdb_lookup l JOIN movie_registry m USING (movie_key)
        WHERE l.response_json IS NOT NULL
          AND l.lookup_status IN ('matched', 'ambiguous', 'not_found')
        ORDER BY l.fetched_at_utc DESC, m.movie_key
    """).fetchall():
        if (title, year) not in cached_keys:
            con.execute('INSERT INTO omdb_query_cache VALUES (?, ?, ?, ?)',
                        [title, year, response, fetched_at])
            cached_keys.add((title, year))


class RequestCoordinator:
    """The calling thread owns DuckDB, reservations and the bounded HTTP window."""

    def __init__(self, con, executor, api_key, fetcher, clock, limit, daily_cap,
                 retry_not_found, counts):
        self.con, self.executor = con, executor
        self.api_key, self.fetcher, self.clock = api_key, fetcher, clock
        self.limit, self.daily_cap = limit, daily_cap
        self.retry_not_found, self.counts = retry_not_found, counts
        self.active = {}
        self.stopped = False
        self.request_statuses = Counter()
        self.cache = {(title, year): (response, fetched_at)
                      for title, year, response, fetched_at in con.execute(
                          'SELECT * FROM omdb_query_cache').fetchall()}

    def now(self):
        now = self.clock()
        if now.tzinfo is not None:
            now = now.astimezone(timezone.utc).replace(tzinfo=None)
        return now

    def used_budget(self, now):
        # Inclusive boundary preserves the existing rolling-window convention.
        return self.con.execute(
            'SELECT count(*) FROM omdb_request_log WHERE requested_at_utc >= ?',
            [now - timedelta(hours=24)]).fetchone()[0]

    def observe_fatal(self):
        # Inspect completed responses before submitting anything else, even when
        # an earlier logical candidate is still running. Persistence stays ordered.
        for query in self.active.values():
            if query.future.done():
                result = query.future.result()
                if _classification(result, query.owner)[0] in FATAL_STATUSES:
                    self.stopped = True

    def request(self, candidate, year=None):
        key = (candidate.title, year or 0)
        if not self.retry_not_found:
            cached = self.cache.get(key)
            if cached:
                self.counts['cache_reused'] += 1
                return Query(candidate, year, cached[1], result=FetchResult(json.loads(cached[0])))
            if key in self.active:
                self.counts['cache_reused'] += 1
                self.counts['coalesced'] += 1
                return self.active[key]
        self.observe_fatal()
        if self.stopped or not self.api_key or self.counts['requests'] >= self.limit:
            return None
        now = self.now()
        if self.used_budget(now) >= self.daily_cap:
            self.counts['budget_stop'] += 1
            return None
        request_id = str(uuid4())
        # Autocommit before submit: a crash or rollback cannot refund a request.
        self.con.execute("""
            INSERT INTO omdb_request_log
            (request_id, movie_key, requested_at_utc, request_status, http_status, request_kind)
            VALUES (?, ?, ?, 'reserved', NULL, ?)
        """, [request_id, candidate.movie_key, now,
              'title' if year is None else f'title_year:{year}'])
        self.counts['requests'] += 1
        query = Query(candidate, year, now, request_id=request_id)
        query.future = self.executor.submit(_fetch, self.fetcher, candidate.title, self.api_key, year)
        # Retry-not-found deliberately bypasses both persistent and shared cache.
        self.active[request_id if self.retry_not_found else key] = query
        return query

    def resolve(self, query, candidate):
        if query.result is None:
            result = query.future.result()
            status, _ = _classification(result, query.owner)
            self.con.execute('BEGIN TRANSACTION')
            try:
                self.con.execute(
                    'UPDATE omdb_request_log SET request_status = ?, http_status = ? WHERE request_id = ?',
                    [status, result.http_status, query.request_id])
                if result.payload is not None and status in CACHEABLE_STATUSES:
                    response_json = json.dumps(result.payload, ensure_ascii=False)
                    self.con.execute("""
                        INSERT INTO omdb_query_cache VALUES (?, ?, ?, ?)
                        ON CONFLICT (source_title, query_year) DO UPDATE SET
                            response_json = excluded.response_json,
                            fetched_at_utc = excluded.fetched_at_utc
                    """, [query.owner.title, query.year or 0,
                          response_json, query.fetched_at])
                self.con.execute('COMMIT')
            except BaseException:
                self.con.execute('ROLLBACK')
                raise
            query.result = result
            if result.payload is not None and status in CACHEABLE_STATUSES:
                self.cache[(query.owner.title, query.year or 0)] = (response_json, query.fetched_at)
            key = query.request_id if self.retry_not_found else (query.owner.title, query.year or 0)
            del self.active[key]
            self.counts[status] += 1  # Keep the existing request counters compatible.
            self.request_statuses[status] += 1
            if status in FATAL_STATUSES:
                self.stopped = True
        status, reason = _classification(query.result, candidate)
        return query.result.payload, status, reason, query.fetched_at


def _save_period(con, candidate, result, query_year):
    payload, status, reason, fetched_at = result
    con.execute('BEGIN TRANSACTION')
    try:
        if payload is not None:
            _save_response(con, candidate.movie_key, payload, status, reason, fetched_at, query_year)
        elif status == 'retry_later':
            con.execute('UPDATE movie_metadata SET match_status = ? WHERE movie_key = ?',
                        [status, candidate.movie_key])
        con.execute('COMMIT')
    except BaseException:
        con.execute('ROLLBACK')
        raise


def enrich(db_path: Path, api_key: str | None, *, limit: int = 2,
           daily_cap: int = MAX_DAILY_CAP, retry_not_found: bool = False,
           retry_ambiguous: bool = False, fetcher=fetch_title, workers: int = 1,
           clock=_utcnow, refresh: bool = True) -> dict:
    if not 1 <= limit <= MAX_DAILY_CAP or not 1 <= daily_cap <= MAX_DAILY_CAP:
        raise ValueError('limit and daily-cap must be between 1 and 900')
    if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
        raise ValueError('workers must be a positive integer')
    if not db_path.is_file():
        raise FileNotFoundError('Run the CSV loader first')
    if retry_not_found and retry_ambiguous:
        raise ValueError('Choose either retry-not-found or retry-ambiguous')
    counts, period_statuses = Counter(), Counter()
    with duckdb.connect(str(db_path)) as con:
        from pipeline.state import migrate
        migrate(con)
        candidates = iter(_candidates(con, retry_not_found, retry_ambiguous))
        _upgrade_cache(con)
        executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='omdb')
        coordinator = RequestCoordinator(con, executor, api_key, fetcher, clock, limit,
                                         daily_cap, retry_not_found, counts)
        pending = deque()
        exhausted = False
        try:
            while True:
                while (not exhausted and not coordinator.stopped
                       and len(coordinator.active) < workers
                       and (not pending or pending[0][1].result is None)):
                    candidate = next(candidates, None)
                    if candidate is None:
                        exhausted = True
                        break
                    query = coordinator.request(candidate)
                    if query is not None:
                        pending.append((candidate, query))
                        # Resolve a cached result now, so its fallback has priority
                        # over title requests that have not yet been submitted.
                        if query.result is not None:
                            break
                if not pending:
                    break
                candidate, query = pending.popleft()
                result = coordinator.resolve(query, candidate)
                payload, status, reason, _ = result
                if status in FATAL_STATUSES:
                    continue
                query_year = None
                if (status == 'ambiguous' and reason == 'Release year needs review'
                        and not 0 <= candidate.first_year - int(payload['Year']) <= 2):
                    fallback = coordinator.request(candidate, candidate.first_year)
                    if fallback is not None:
                        fallback_result = coordinator.resolve(fallback, candidate)
                        if fallback_result[1] == 'matched':
                            result, query_year = fallback_result, candidate.first_year
                _save_period(con, candidate, result, query_year)
                period_statuses[result[1]] += 1
        finally:
            # Interruptions never submit more work. Outstanding reservations stay
            # charged; completed period transactions are already durable.
            executor.shutdown(wait=True, cancel_futures=True)
        counts['requests_last_24h'] = coordinator.used_budget(coordinator.now())
    if refresh:
        from pipeline.dbt_runner import run_dbt
        run_dbt(db_path, 'build', '--select', 'tag:marts')
    return dict(counts, request_statuses=dict(coordinator.request_statuses),
                period_statuses=dict(period_statuses))
