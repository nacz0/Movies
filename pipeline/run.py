"""Import a snapshot, build periods, enrich from cache/HTTP, then dbt build + tests."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

import duckdb

from pipeline import ROOT
from pipeline.enrich import enrich, load_api_key, reuse_cache
from pipeline.ingest import import_snapshot, initialize

DEFAULT_DB = ROOT / 'data/movies.duckdb'


def run_dbt(db_path: Path, *arguments: str) -> None:
    identity = hashlib.sha256(str(db_path.resolve()).encode()).hexdigest()[:12]
    environment = dict(os.environ, MOVIES_DBT_PATH=str(db_path.resolve()),
                       DBT_SEND_ANONYMOUS_USAGE_STATS='false', PYTHONIOENCODING='utf-8')
    command = [sys.executable, '-c', 'from dbt.cli.main import cli; cli()',
               '--quiet', '--no-use-colors', *arguments, '--project-dir', str(ROOT / 'dbt'),
               '--profiles-dir', str(ROOT / 'dbt'),
               '--target-path', str(ROOT / 'dbt/target' / identity),
               '--log-path', str(ROOT / 'dbt/logs' / identity)]
    result = subprocess.run(command, env=environment, cwd=ROOT, capture_output=True,
                            text=True, encoding='utf-8', errors='replace')
    if result.returncode:
        raise RuntimeError(f'dbt {" ".join(arguments)} failed:\n' +
                           (result.stdout + result.stderr)[-14000:])


def rebuild(csv_path: Path | None, db_path: Path = DEFAULT_DB, *, api_key=None,
            workers=4, limit=100, daily_cap=900, cache_from: Path | None = None) -> dict:
    # Validate options before import or network access.
    if not 1 <= workers <= 32 or limit < 0 or not 0 <= daily_cap <= 900:
        raise ValueError('Use 1..32 workers, a nonnegative limit and a daily cap of 0..900')
    initialize(db_path)
    if csv_path is not None:
        import_snapshot(csv_path, db_path)
    with duckdb.connect(str(db_path), read_only=True) as con:
        if not con.execute('SELECT count(*) FROM movies_state.raw_revenues').fetchone()[0]:
            raise ValueError('No source data. Supply --csv on the first run.')
    if cache_from is not None:
        reuse_cache(cache_from, db_path)
    # dbt owns period detection and enrichment selection. Close every Python
    # connection before dbt opens the file (required by Windows file locking).
    run_dbt(db_path, 'run', '--select', '+int_omdb_candidates')
    enrichment = enrich(db_path, api_key, workers=workers, limit=limit, daily_cap=daily_cap)
    run_dbt(db_path, 'build')
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows, revenue = con.execute('SELECT count(*), sum(revenue) FROM fact_daily_revenue').fetchone()
        periods = con.execute('SELECT count(*) FROM dim_movie').fetchone()[0]
    return dict(fact_rows=rows, total_revenue=revenue, periods=periods, enrichment=enrichment)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', type=Path, default=DEFAULT_DB)
    parser.add_argument('--csv', type=Path, help='Replace the raw snapshot after validation')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--enrich', action='store_true', help='Allow new OMDb HTTP requests')
    mode.add_argument('--offline', action='store_true', help='Use cached responses only (default)')
    parser.add_argument('--workers', type=int, default=4)
    parser.add_argument('--limit', type=int, default=100, help='Maximum HTTP calls this run')
    parser.add_argument('--daily-cap', type=int, default=900, help='Rolling 24-hour HTTP cap, at most 900')
    parser.add_argument('--reuse-cache-from', type=Path, help='Read-only one-time copy from a legacy database')
    args = parser.parse_args()
    try:
        result = rebuild(args.csv, args.db, api_key=load_api_key() if args.enrich else None,
                         workers=args.workers, limit=args.limit, daily_cap=args.daily_cap,
                         cache_from=args.reuse_cache_from)
    except (ValueError, OSError, duckdb.Error, RuntimeError) as exc:
        parser.exit(1, f'{exc}\n')
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
