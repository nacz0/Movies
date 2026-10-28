"""Build on a working copy, validate with dbt, and publish a complete database."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import shutil
from uuid import uuid4

import duckdb

from pipeline import ROOT
from pipeline.dbt_runner import run_dbt
from pipeline.state import migrate, summary, synchronize


def working_path(db_path: Path) -> Path:
    return db_path.with_name(db_path.stem + '.working' + db_path.suffix)


@contextmanager
def database_lock(db_path: Path):
    """One publisher per active file, including the copy and publication steps."""
    lock = db_path.with_name(db_path.name + '.pipeline.lock')
    try:
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError:
        raise RuntimeError(f'Pipeline lock exists: {lock}. See README for crash recovery.') from None
    try:
        with os.fdopen(descriptor, 'w') as handle:
            handle.write(str(os.getpid()))
        yield
    finally:
        lock.unlink()


def _checkpoint(db_path: Path) -> None:
    # Opening read/write also proves a dashboard reader is not holding the file.
    with duckdb.connect(str(db_path)) as con:
        con.execute('CHECKPOINT')


def _publish(work: Path, active: Path) -> None:
    _checkpoint(work)
    if active.exists():
        _checkpoint(active)
        backup = active.with_name(active.name + '.backup.duckdb')
        # Copy first; active remains available even if backup creation fails.
        pending_backup = active.with_name(active.name + '.backup-next.duckdb')
        shutil.copy2(active, pending_backup)
        os.replace(pending_backup, backup)
    os.replace(work, active)


def _transform(work, csv_path, enrichment, target, full_refresh):
    with duckdb.connect(str(work)) as con:
        migrate(con)
    if csv_path is not None:
        from pipeline.import_revenues import import_snapshot
        import_snapshot(csv_path, work)
    with duckdb.connect(str(work), read_only=True) as con:
        if not con.execute('SELECT count(*) FROM raw_revenues').fetchone()[0]:
            raise ValueError('No source rows. Provide --csv for a new database.')
    refresh = ('--full-refresh',) if full_refresh else ()
    run_dbt(work, 'build', '--select', 'tag:periods', *refresh, target=target)
    with duckdb.connect(str(work)) as con:
        synchronize(con)
    run_dbt(work, 'build', '--select', 'tag:candidates', target=target)
    enrichment_result = None
    if enrichment is not None:
        from pipeline.enrich_omdb import enrich
        enrichment_result = enrich(work, **enrichment, refresh=False)
    run_dbt(work, 'build', *refresh, target=target)
    result = summary(work)
    if enrichment_result is not None:
        result['enrichment'] = enrichment_result
    return result


def rebuild(csv_path: Path | None, db_path: Path, *, resume: bool = False,
            enrichment: dict | None = None, target: str = 'dev',
            full_refresh: bool = False) -> dict:
    db_path = db_path.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    work = working_path(db_path)
    with database_lock(db_path):
        if db_path.exists():
            _checkpoint(db_path)
        if work.exists() and not resume:
            raise RuntimeError(f'Unfinished working database: {work}. Resume with --resume; '
                               'retain its request ledger and cache.')
        if resume and not work.is_file():
            raise FileNotFoundError(f'No working database to resume: {work}')
        if not resume and db_path.exists():
            shutil.copy2(db_path, work)
        result = _transform(work, csv_path, enrichment, target, full_refresh)
        _publish(work, db_path)
    return result


def restore(db_path: Path, backup: Path | None = None, *, target='dev') -> dict:
    """Restore analytical data while carrying forward every spent API request."""
    db_path = db_path.resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    backup = (backup or db_path.with_name(db_path.name + '.backup.duckdb')).resolve()
    if not backup.is_file() or backup == db_path or backup == working_path(db_path):
        raise ValueError('Choose an existing backup distinct from active and working databases')
    candidate = db_path.with_name(db_path.stem + '.restoring.duckdb')
    with database_lock(db_path):
        if candidate.exists():
            raise RuntimeError(f'Restore candidate already exists; retain it for recovery: {candidate}')
        _checkpoint(backup)
        shutil.copy2(backup, candidate)
        with duckdb.connect(str(candidate)) as con:
            migrate(con)
            for donor in (db_path, working_path(db_path)):
                if not donor.exists():
                    continue
                _checkpoint(donor)
                with duckdb.connect(str(donor), read_only=True) as saved:
                    requests = saved.execute('SELECT * FROM omdb_request_log').fetchall()
                    cache = saved.execute('SELECT * FROM omdb_query_cache').fetchall()
                # UUIDs deduplicate request reservations across copies. A
                # completed status may replace reserved, never the reverse.
                if requests:
                    con.executemany('''INSERT INTO omdb_request_log VALUES (?,?,?,?,?,?)
                        ON CONFLICT (request_id) DO UPDATE SET
                            request_status=excluded.request_status,
                            http_status=excluded.http_status
                        WHERE excluded.request_status <> 'reserved' ''', requests)
                if cache:
                    con.executemany('''INSERT INTO omdb_query_cache VALUES (?,?,?,?)
                        ON CONFLICT (source_title,query_year) DO UPDATE SET
                            response_json=excluded.response_json,
                            fetched_at_utc=excluded.fetched_at_utc
                        WHERE excluded.fetched_at_utc >= omdb_query_cache.fetched_at_utc''', cache)
        result = _transform(candidate, None, None, target, False)
        _publish(candidate, db_path)
        work = working_path(db_path)
        if work.exists():
            archive = db_path.with_name(db_path.stem + f'.failed-{uuid4().hex}.duckdb')
            os.replace(work, archive)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=ROOT / 'data/box_office.duckdb')
    parser.add_argument('--csv', type=Path, help='Import this snapshot; omit for metadata-only rebuild')
    parser.add_argument('--resume', action='store_true', help='Retain state and ledger from a failed run')
    parser.add_argument('--restore', action='store_true', help='Restore backup, retaining API ledger and cache')
    parser.add_argument('--backup', type=Path, help='Backup for --restore; defaults to the last publication backup')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--enrich', action='store_true', help='Use configured OMDb key')
    mode.add_argument('--offline', action='store_true', help='Reclassify cached responses without network')
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--daily-cap', type=int, default=900)
    parser.add_argument('--workers', type=int, default=1)
    retry = parser.add_mutually_exclusive_group()
    retry.add_argument('--retry-not-found', action='store_true')
    retry.add_argument('--retry-ambiguous', action='store_true')
    parser.add_argument('--target', choices=('dev', 'ci'), default='dev')
    parser.add_argument('--full-refresh', action='store_true')
    args = parser.parse_args()
    if args.restore:
        if args.csv or args.resume or args.enrich or args.offline or args.full_refresh or args.retry_not_found or args.retry_ambiguous:
            parser.error('--restore cannot import or enrich; it restores and validates the backup')
        print(json.dumps(restore(args.db, args.backup, target=args.target), indent=2))
        return
    if args.backup:
        parser.error('--backup requires --restore')
    options = None
    if args.enrich or args.offline:
        from pipeline.enrich_omdb import load_api_key
        options = dict(api_key=load_api_key() if args.enrich else None,
                       limit=args.limit, daily_cap=args.daily_cap, workers=args.workers,
                       retry_not_found=args.retry_not_found, retry_ambiguous=args.retry_ambiguous)
    elif args.retry_not_found or args.retry_ambiguous:
        parser.error('Retry requires --enrich or --offline')
    print(json.dumps(rebuild(args.csv, args.db, resume=args.resume, enrichment=options,
                             target=args.target, full_refresh=args.full_refresh), indent=2))


if __name__ == '__main__':
    main()
