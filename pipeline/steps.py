"""Small, independently executable stages for the Movies Airflow DAG."""

import argparse
import json
from pathlib import Path

import duckdb

from pipeline import ROOT
from pipeline.enrich import enrich
from pipeline.ingest import import_snapshot
from pipeline.run import run_dbt

DEFAULT_DB = ROOT / 'data/movies_steps.duckdb'
STEPS = ('ingest', 'dbt_prepare', 'enrich_omdb', 'dbt_build', 'validate')


def validate(db_path: Path, *, expected_rows=337818, expected_revenue=205110995141,
             expected_periods=6661) -> dict:
    """Read-only acceptance checks for the complete CSV and offline execution."""
    with duckdb.connect(str(db_path), read_only=True) as con:
        rows, revenue = con.execute(
            'SELECT count(*), sum(revenue) FROM fact_daily_revenue').fetchone()
        periods = con.execute('SELECT count(*) FROM dim_movie').fetchone()[0]
        raw = con.execute(
            'SELECT count(*), sum(revenue) FROM movies_state.raw_revenues').fetchone()
        prepared_periods = con.execute(
            'SELECT count(*) FROM int_reporting_periods').fetchone()[0]
        requests = con.execute('SELECT count(*) FROM movies_state.omdb_requests').fetchone()[0]
    actual = (rows, revenue, periods)
    expected = (expected_rows, expected_revenue, expected_periods)
    if actual != expected or raw != (rows, revenue) or prepared_periods != periods:
        raise ValueError(f'Dataset validation failed: facts/revenue/periods={actual}, '
                         f'expected={expected}, raw={raw}, prepared_periods={prepared_periods}')
    if requests:
        raise ValueError(f'Offline database contains {requests} OMDb request reservations')
    return dict(fact_rows=rows, total_revenue=revenue, periods=periods, omdb_requests=requests)


def run_step(step: str, db_path: Path = DEFAULT_DB, *, csv_path: Path | None = None) -> dict:
    if step == 'ingest':
        return import_snapshot(csv_path if csv_path is not None else ROOT / 'revenues_per_day.csv',
                               db_path)
    if step == 'dbt_prepare':
        run_dbt(db_path, 'run', '--select', '+int_omdb_candidates')
        return {'dbt': 'reporting periods and OMDb candidates prepared'}
    if step == 'enrich_omdb':
        # Never load .env or an API key. Both request budgets are also zero.
        result = enrich(db_path, api_key=None, workers=4, limit=0, daily_cap=0)
        if result['requests'] != 0:
            raise ValueError('Offline enrichment unexpectedly reserved an OMDb request')
        return result
    if step == 'dbt_build':
        run_dbt(db_path, 'build')
        return {'dbt': 'complete build and tests passed'}
    if step == 'validate':
        return validate(db_path)
    raise ValueError(f'Unknown pipeline step: {step}')


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('step', choices=STEPS)
    parser.add_argument('--db', type=Path, default=DEFAULT_DB,
                        help='Use an isolated database for development; the DAG uses movies_steps.duckdb')
    parser.add_argument('--csv', type=Path, help='Input snapshot (ingest only)')
    args = parser.parse_args()
    if args.csv is not None and args.step != 'ingest':
        parser.error('--csv is only valid for ingest')
    print(f'Starting {args.step}: database={args.db.resolve()}', flush=True)
    # Exceptions intentionally escape: BashOperator must receive a nonzero exit code.
    result = run_step(args.step, args.db, csv_path=args.csv)
    print(json.dumps(dict(step=args.step, **result)), flush=True)


if __name__ == '__main__':
    main()
