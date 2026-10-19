"""Compare business tables to a sequential Git revision, excluding IDs/times in request history."""

import argparse
import importlib.util
import json
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

from benchmarks.benchmark_pipeline import FIXTURE, Fetcher, payload
from pipeline.enrich_omdb import enrich
from pipeline.load_revenues import ROOT, load


def business_state(path):
    with duckdb.connect(str(path)) as con:
        return {name: con.execute(sql).fetchall() for name, sql in {
            'movies': 'SELECT * FROM dim_movie ORDER BY movie_key',
            'facts': 'SELECT * FROM fact_daily_revenue ORDER BY source_id',
            'dates': 'SELECT * FROM dim_date ORDER BY date_key',
            'distributors': 'SELECT * FROM dim_distributor ORDER BY distributor_key',
            'lookup': 'SELECT movie_key, lookup_status, candidate_imdb_id, candidate_title, '
                      'match_reason, response_json, query_year FROM omdb_lookup ORDER BY movie_key',
            'cache': 'SELECT source_title, query_year, response_json FROM omdb_query_cache ORDER BY 1,2',
        }.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', default='ad01f37')
    parser.add_argument('--output', type=Path, default=ROOT / 'benchmarks/results/equivalence.json')
    args = parser.parse_args()
    results = []
    with tempfile.TemporaryDirectory(prefix='movies-equivalence-') as directory:
        temp = Path(directory)
        module_path = temp / 'baseline.py'
        module_path.write_bytes(subprocess.check_output(
            ['git', 'show', f'{args.revision}:pipeline/enrich_omdb.py'], cwd=ROOT))
        spec = importlib.util.spec_from_file_location('sequential_baseline', module_path)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)
        source = temp / 'source.csv'
        source.write_text('id,date,title,revenue,theaters,distributor\n' + '\n'.join(
            f'{i}-{p},{2020+p}-01-01,Fixture {i:03},{1000-i-p},10,Studio'
            for i in range(12) for p in range(2)) + '\n', encoding='utf-8')
        snapshot = temp / 'snapshot.duckdb'
        load(source, snapshot)
        years = {f'Fixture {i:03}': FIXTURE['base_year'] for i in range(12)}
        for mode in ('cold', 'partial', 'offline', 'errors', 'retry_not_found', 'retry_ambiguous'):
            seed = temp / 'seed.duckdb'
            shutil.copyfile(snapshot, seed)
            with duckdb.connect(str(seed)) as con:
                for i, title in enumerate(years):
                    if mode in ('offline', 'retry_not_found', 'retry_ambiguous') or (mode == 'partial' and i % 2 == 0):
                        con.execute('INSERT INTO omdb_query_cache VALUES (?,0,?,current_timestamp)',
                                    [title, json.dumps(payload(title))])
                if mode.startswith('retry_'):
                    con.execute('UPDATE dim_movie SET match_status=?', [mode.removeprefix('retry_')])
            expected = None
            for workers in (0, 1, 2, 4, 8):
                db = temp / 'run.duckdb'
                shutil.copyfile(seed, db)
                fetcher = Fetcher(0, mode, years)
                kwargs = {mode: True} if mode.startswith('retry_') else {}
                if workers:
                    kwargs['workers'] = workers
                (enrich if workers else module.enrich)(
                    db, None if mode == 'offline' else 'fixture', limit=100, fetcher=fetcher, **kwargs)
                actual = business_state(db)
                if expected is None:
                    expected = actual
                else:
                    mismatches = [name for name in expected if expected[name] != actual[name]]
                    if mismatches:
                        raise AssertionError((mode, workers, mismatches))
                    results.append(dict(scenario=mode, workers=workers, business_tables_equal=True,
                                        actual_calls=sum(fetcher.calls.values())))
                if mode == 'offline':
                    assert not fetcher.calls
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(dict(baseline_revision=args.revision, comparisons=results), indent=2) + '\n')
    print(f'{len(results)} comparisons passed; {args.output}')


if __name__ == '__main__':
    main()
