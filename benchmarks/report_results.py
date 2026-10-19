"""Validate benchmark acceptance checks and write a local diagnostic report."""

import argparse
import json
from pathlib import Path

from pipeline.load_revenues import ROOT


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, default=ROOT / 'benchmarks/results/baseline.json')
    parser.add_argument('--final', type=Path, default=ROOT / 'benchmarks/results/concurrency.json')
    parser.add_argument('--equivalence', type=Path, default=ROOT / 'benchmarks/results/equivalence.json')
    parser.add_argument('--output', type=Path, default=ROOT / 'benchmarks/results/summary.md')
    args = parser.parse_args()
    baseline = json.loads(args.baseline.read_text())
    final = json.loads(args.final.read_text())
    equivalence = json.loads(args.equivalence.read_text())
    for field in ('python', 'duckdb', 'platform', 'cpu_count', 'duckdb_threads', 'csv_sha256',
                  'fixture_sha256', 'sample_csv_sha256', 'seed', 'sample_size', 'repeats',
                  'warmups', 'limit', 'daily_cap', 'latencies_ms'):
        assert baseline['metadata'][field] == final['metadata'][field], field
    for report in (baseline, final):
        assert report['metadata']['repeats'] >= 5 and report['metadata']['warmups'] >= 1
        titles = report['metadata']['sample_size']
        for run in report['runs']:
            assert run['actual_calls'] <= report['metadata']['limit']
            assert run['counts'].get('requests_last_24h', 0) <= report['metadata']['daily_cap']
            if run['scenario'] in ('first_import', 'reimport', 'pipeline'):
                assert run['facts'] == [337818, 205110995141, 161]
            if run['scenario'] in ('cold', 'partial', 'offline'):
                assert run['actual_calls'] == {'cold': titles, 'partial': titles // 2, 'offline': 0}[run['scenario']]
                assert run['period_statuses'] == {'matched': titles * 2}
            if run['scenario'] in ('invalid_key', 'quota_exhausted'):
                assert 1 <= run['actual_calls'] <= run['workers']
    assert all(row['business_tables_equal'] for row in equivalence['comparisons'])
    index = lambda report: {(r['scenario'], r['workers'], r['latency_ms']): r for r in report['summary']}
    before, after = index(baseline), index(final)
    target = after['cold', 1, 200]['median_seconds'] / after['cold', 4, 200]['median_seconds']
    assert target >= 2, f'200 ms speedup target failed: {target:.2f}x'
    lines = ['# Concurrency benchmark results', '',
        'Measured on 19 October 2026 using deterministic fixtures, without network access. '
        'The original sequential implementation is compared with the implementation on branch `concurrency`. '
        'One warmup and five measured repetitions were used for every configuration; database preparation and copies are excluded.', '',
        f"Environment: Python {final['metadata']['python']}, DuckDB {final['metadata']['duckdb']}, "
        f"{final['metadata']['platform']}, {final['metadata']['cpu_count']} logical CPUs, "
        f"{final['metadata']['duckdb_threads']} DuckDB threads. Seed: `{final['metadata']['seed']}`. "
        f"Synthetic scenarios use {final['metadata']['sample_size']} independent titles and twice as many periods, "
        f"a run limit of {final['metadata']['limit']} and a rolling daily cap of {final['metadata']['daily_cap']}.", '',
        f"Baseline commit: `{baseline['metadata']['commit']}`. Both measurements were made from a dirty working tree; "
        'the implementation source hashes in the raw reports identify the exact measured code. '
        'The loader and dashboard were not modified.', '',
        '## Empty-cache scaling', '',
        'Median wall time in seconds. Speedup compares final workers=1 against final workers=4; '
        'each run makes exactly one title request per independent title.', '',
        '| Fetch delay | Original sequential | Final 1 worker | 2 workers | 4 workers | 8 workers | 4-worker speedup |',
        '| --- | ---: | ---: | ---: | ---: | ---: | ---: |']
    for delay in final['metadata']['latencies_ms']:
        old = before['cold', 1, delay]['median_seconds']
        values = [after['cold', w, delay]['median_seconds'] for w in (1, 2, 4, 8)]
        lines.append(f'| {delay} ms | {old:.3f} | ' + ' | '.join(f'{v:.3f}' for v in values)
                     + f' | {values[0] / values[2]:.2f}× |')
    lines += ['', f'The 200 ms acceptance target of at least 2× with four workers was met: **{target:.2f}×**. '
        'These are end-to-end fixture enrichment times, including coordinator and persistence overhead; '
        'they are not a prediction of real OMDb performance.', '',
        '## Sequential cost and other stages', '',
        'Times are medians with the observed minimum–maximum in parentheses. The final column compares final workers=1 with the original version.', '',
        '| Scenario | Baseline seconds (range) | Final 1 worker seconds (range) | Change |',
        '| --- | ---: | ---: | ---: |']
    regressions = []
    for (scenario, workers, delay), old in before.items():
        new = after[scenario, workers, delay]
        change = (new['median_seconds'] / old['median_seconds'] - 1) * 100
        label = f'{scenario} ({delay} ms)' if delay else scenario
        fmt = lambda x: f"{x['median_seconds']:.3f} ({x['min_seconds']:.3f}–{x['max_seconds']:.3f})"
        lines.append(f'| {label} | {fmt(old)} | {fmt(new)} | {change:+.1f}% |')
        if change > 10:
            regressions.append((label, old, new))
    lines += ['', 'The coordinator snapshots the query cache in memory and updates it only after committed responses. '
        'This avoids repeated cache SQL queries while scanning candidates after the request budget is spent. '
        'It changes enrichment traversal cost; the CSV import transaction remains unchanged.', '']
    if regressions:
        lines += ['Configurations above the 10% investigation threshold:', '']
        for label, old, new in regressions:
            overlap = max(old['min_seconds'], new['min_seconds']) <= min(old['max_seconds'], new['max_seconds'])
            lines.append(f'- {label}: observed ranges {"overlap" if overlap else "do not overlap"}. '
                         'Treat this as a measured regression requiring scrutiny; fixture timings include thread and database overhead.')
        lines.append('')
    else:
        lines += ['No import or workers=1 median regressed by more than 10% in this run.', '']
    lines += ['## Correctness and limits', '',
        f"- {len(equivalence['comparisons'])} comparisons against the original sequential implementation passed for "
        'cold/partial/offline/error scenarios and both retry modes, with workers 1/2/4/8 and sufficient budget. '
        'All business columns in movies, lookups, cache, facts, dates and distributors agree; fetch timestamps and request IDs are excluded.',
        '- Every cold/partial/offline run made exactly 24/12/0 fetches respectively and matched all 48 fixture periods. '
        'These checks also verify shared title requests are not duplicated.',
        '- All full-data runs retained 337,818 facts, $205,110,995,141 revenue and 161 missing theater counts.',
        '- Every benchmark run stayed within its run and daily budgets. Fatal scenarios submitted no more than the worker window. '
        'The 29-test suite also covers reverse completion order, active request bounds, fallback priority, rolling-window boundaries, '
        'both retry modes, interruption/resume, and transaction rollback without refunding reservations.',
        '- With tight budgets, already reserved titles can consume the slots a sequential run would use for fallback. '
        'Business-table equivalence is asserted with sufficient budget; limited-budget ordering is tested separately.',
        '- The original baseline was measured before the final worker matrix. Early baseline rounds overlapped short local test runs; '
        'the recorded ranges show machine noise. Filesystem cache is uncontrolled. These observations do not establish production tail latency. '
        'Fetch p95 values have only small samples and are descriptive.', '',
        '## Reproduction', '',
        'See the repository README for reproduction commands and business conclusions.', '',
        'Generated JSON/CSV reports in `benchmarks/results/` stay local and are ignored by Git. '
        'Run the documented commands to recreate `baseline.json`, `concurrency.json`, their CSV exports and `equivalence.json`.', '',
        'These local reports contain individual runs, stage times (including combined import + enrichment), rates, cache/fallback counts, '
        'request and period statuses, latency p50/p95/sample sizes, medians, ranges, standard deviations and reproducibility metadata.', '']
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text('\n'.join(lines), encoding='utf-8')
    print(f'Acceptance checks passed; 200 ms / 4 workers: {target:.2f}x; {args.output}')


if __name__ == '__main__':
    main()
