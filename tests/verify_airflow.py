"""Optional Airflow-environment checks; not part of the Movies dependency set.

Run with /home/nacho/airflow/.venv/bin/python -m tests.verify_airflow.
Add --run for a complete local DAG run or --failure for an isolated negative control.
"""

import argparse
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]
TASKS = ['ingest', 'dbt_prepare', 'enrich_omdb', 'dbt_build', 'validate']


def load_dag(path):
    from airflow.dag_processing.dagbag import DagBag

    bag = DagBag(dag_folder=str(path))
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags['movies_steps']
    assert list(dag.task_dict) == TASKS, list(dag.task_dict)
    assert dag.catchup is False and dag.max_active_runs == 1 and dag.max_active_tasks == 1
    assert dag.schedule is None
    for i, name in enumerate(TASKS):
        task = dag.get_task(name)
        assert task.upstream_task_ids == (set(TASKS[i - 1:i]) if i else set())
        assert task.downstream_task_ids == set(TASKS[i + 1:i + 2])
        assert task.trigger_rule == 'all_success' and task.do_xcom_push is False
        assert task.skip_on_exit_code == []
        assert '"$MOVIES_REPO/.venv/bin/python" -m pipeline.steps ' + name in task.bash_command
        assert task.execution_timeout == timedelta(minutes=30 if name.startswith('dbt_') else 10)
        assert task.retries == (2 if name.startswith('dbt_') else 0)
    print('DAG parse and configuration checks passed', flush=True)
    return dag


def run_and_check(dag, *, failure=False):
    run = dag.test()
    instances = run.get_task_instances()
    states = {ti.task_id: str(ti.state) for ti in instances}
    print(json.dumps({'dag_state': str(run.state), 'tasks': states}), flush=True)
    if failure:
        assert str(run.state) == 'failed', run.state
        assert states == dict(ingest='failed', **{name: 'upstream_failed' for name in TASKS[1:]}), states
        # Airflow timestamps upstream_failed tasks even though they never execute.
        assert all(ti.try_number == 0 for ti in instances if ti.task_id != 'ingest')
    else:
        assert str(run.state) == 'success', run.state
        assert states == {name: 'success' for name in TASKS}, states


def failure_check():
    # An isolated metadata DB/bundle prevents test changes from reaching running Airflow.
    with tempfile.TemporaryDirectory(prefix='movies-airflow-failure-') as temporary:
        home = Path(temporary)
        dags = home / 'dags'
        dags.mkdir()
        source = (ROOT / 'dags/movies_steps.py').read_text(encoding='utf-8')
        source += (f"\ningest.bash_command += ' --csv {home}/missing.csv"
                   f" --db {home}/failure.duckdb'\n")
        (dags / 'movies_steps.py').write_text(source, encoding='utf-8')
        environment = dict(os.environ, AIRFLOW_HOME=str(home),
                           AIRFLOW__CORE__DAGS_FOLDER=str(dags),
                           AIRFLOW__CORE__LOAD_EXAMPLES='false',
                           AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f'sqlite:///{home}/airflow.db',
                           AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST=json.dumps([
                               {'name': 'dags-folder',
                                'classpath': 'airflow.dag_processing.bundles.local.LocalDagBundle',
                                'kwargs': {}}]))
        subprocess.run([sys.executable, '-m', 'airflow', 'db', 'migrate'],
                       env=environment, check=True)
        subprocess.run([sys.executable, str(Path(__file__).resolve()),
                        '--failure-worker', str(dags)], env=environment, check=True)
        assert not (home / 'failure.duckdb').exists(), 'Failed ingest wrote a database'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--run', action='store_true')
    mode.add_argument('--failure', action='store_true')
    mode.add_argument('--failure-worker', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.failure:
        failure_check()
    else:
        dag = load_dag(args.failure_worker or ROOT / 'dags/movies_steps.py')
        if args.run or args.failure_worker:
            run_and_check(dag, failure=bool(args.failure_worker))


if __name__ == '__main__':
    main()
