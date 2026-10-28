"""Validate dbt source reconciliation, repeatability and migration of saved state."""

import argparse
import json
from pathlib import Path
import shutil
import time

import duckdb

from pipeline import ROOT
from pipeline.dbt_runner import run_dbt
from pipeline.run import rebuild
from pipeline.state import migrate
from dashboard import queries

MARTS = {'dim_date', 'dim_movie', 'dim_distributor', 'fact_daily_revenue'}


def _identifier(name):
    return '"' + name.replace('"', '""') + '"'


def main_tables(path):
    with duckdb.connect(str(path), read_only=True) as con:
        return [row[0] for row in con.execute("""SELECT table_name
            FROM information_schema.tables WHERE table_catalog=current_database()
              AND table_schema='main' AND table_type='BASE TABLE'
            ORDER BY table_name""").fetchall()]


def compare(expected, actual, mapping, columns=None):
    """Compare stored rows and types, optionally projecting a migration contract."""
    with duckdb.connect(str(actual), read_only=True) as con:
        path = str(expected.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{path}' AS expected (READ_ONLY)")
        for target, source in mapping.items():
            actual_columns = con.execute("""SELECT column_name,data_type
                FROM information_schema.columns WHERE table_catalog=current_database()
                  AND table_schema='main' AND table_name=? ORDER BY ordinal_position""",
                [target]).fetchall()
            expected_columns = dict(con.execute("""SELECT column_name,data_type
                FROM information_schema.columns WHERE table_catalog='expected'
                  AND table_schema='main' AND table_name=?""", [source]).fetchall())
            selected = (columns or {}).get(target, [name for name, _ in actual_columns])
            actual_types = dict(actual_columns)
            if not selected or any(name not in expected_columns or
                    actual_types.get(name) != expected_columns[name] for name in selected):
                raise AssertionError(f'{target}: migration column contract changed')
            if source == target and not (columns or {}).get(target):
                if dict(actual_columns) != expected_columns:
                    raise AssertionError(f'{target}: columns changed')
            projection = ', '.join(_identifier(name) for name in selected)
            left = f'SELECT {projection} FROM main.{_identifier(target)}'
            right = f'SELECT {projection} FROM expected.main.{_identifier(source)}'
            count = con.execute(f"""SELECT count(*) FROM (
                ({left} EXCEPT ALL {right}) UNION ALL ({right} EXCEPT ALL {left})
            )""").fetchone()[0]
            if count:
                raise AssertionError(f'{target}: {count} different rows')


def snapshot(source, destination):
    with duckdb.connect(str(source.resolve()), read_only=True):
        shutil.copy2(source, destination)


def dashboard_state(path: Path) -> dict:
    result = {}
    with duckdb.connect(str(path), read_only=True) as con:
        start,end = queries.date_bounds(con)
        for name in ('overview','enrichment_status','movie_ranking','film_ranking',
                     'distributor_ranking','monthly_trend'):
            result[name] = getattr(queries,name)(con,start,end)
        for distributors,genre in [(None,None), (['Walt Disney Studios Motion Pictures'],None),
                                    (None,'Drama')]:
            key = repr((distributors,genre))
            result[key] = queries.overview(con,start,end,distributors,genre)
            result[key+'periods'] = queries.movie_ranking(con,start,end,distributors,genre,
                                                        limit=10,sort='movie',direction='desc')
            result[key+'films'] = queries.film_ranking(con,start,end,distributors,genre,
                                                      limit=10,sort='revenue',direction='asc')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv', type=Path, default=ROOT / 'revenues_per_day.csv')
    parser.add_argument('--db-snapshot', type=Path,
                        help='Optional saved database; only a copy is migrated, using the same CSV')
    parser.add_argument('--target', choices=('dev', 'ci'), default='dev')
    parser.add_argument('--output-dir', type=Path,
                        default=ROOT / 'benchmarks/results/dbt-check')
    args = parser.parse_args()
    out = args.output_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    actual = out / 'dbt.duckdb'
    start = time.perf_counter()
    results = {'summary': rebuild(args.csv, actual, target=args.target)}
    results['first_build_seconds'] = time.perf_counter() - start
    results['first_build_bytes'] = actual.stat().st_size
    expected = out / 'snapshot.duckdb'
    snapshot(actual, expected)
    before_dashboard = dashboard_state(actual)
    # Import history records each successful import; all business state must stay stable.
    stable = {table: table for table in main_tables(expected) if table != 'import_history'}
    start = time.perf_counter()
    repeated = rebuild(args.csv, actual, target=args.target, full_refresh=True)
    results['repeat_full_refresh_seconds'] = time.perf_counter() - start
    if results['summary'] != repeated:
        raise AssertionError('Full refresh changed the summary')
    compare(expected, actual, stable)
    if before_dashboard != dashboard_state(actual):
        raise AssertionError('Full refresh changed dashboard results')
    results['full_refresh_state_and_dashboard_equal'] = True
    results['repeat_bytes'] = actual.stat().st_size

    if args.db_snapshot:
        saved, migrated = out / 'saved.duckdb', out / 'migrated.duckdb'
        snapshot(args.db_snapshot, saved)
        shutil.copy2(saved, migrated)
        tables = main_tables(saved)
        preserved = {table: table for table in tables if table not in MARTS}
        if 'pipeline_migrations' not in tables:
            preserved.update(movie_registry='dim_movie', movie_metadata='dim_movie',
                             distributor_registry='dim_distributor', date_registry='dim_date',
                             revenue_assignments='fact_daily_revenue')
        start = time.perf_counter()
        with duckdb.connect(str(migrated)) as con:
            migrate(con)
        compare(saved, migrated, preserved)
        rebuild(args.csv, migrated, target=args.target)
        results['migration_and_build_seconds'] = time.perf_counter() - start
        # Period labels/end dates are refreshed by dbt; identity and selected API
        # state must retain their saved values after importing the same snapshot.
        preserved.pop('import_history', None)
        compare(saved, migrated, preserved, {
            'movie_registry': ['movie_key', 'source_title', 'run_start_date']})
        expected_migration = out / 'migration_snapshot.duckdb'
        snapshot(migrated, expected_migration)
        run_dbt(migrated, 'build', '--full-refresh', target=args.target)
        compare(expected_migration, migrated,
                {table: table for table in main_tables(expected_migration)})
        if dashboard_state(expected_migration) != dashboard_state(migrated):
            raise AssertionError('Full refresh changed the migrated dashboard')
        results['migration_keys_state_and_full_refresh_equal'] = True

    run_dbt(actual, 'docs', 'generate', target=args.target)
    (out / 'report.json').write_text(json.dumps(results, indent=2, default=str) + '\n',
                                   encoding='utf-8')
    print(json.dumps(results, indent=2, default=str))


if __name__ == '__main__':
    main()
