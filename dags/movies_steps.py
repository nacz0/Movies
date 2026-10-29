"""Airflow 3 orchestration; all pipeline dependencies stay in the Movies venv."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

MOVIES_REPO = '/home/nacho/projects/Movies'

with DAG(
    dag_id='movies_steps',
    schedule=None,
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=['movies', 'dbt', 'offline'],
    default_args={'retries': 0, 'execution_timeout': timedelta(minutes=10)},
    doc_md='Five sequential stages using data/movies_steps.duckdb. OMDb HTTP is disabled.',
) as dag:
    def stage(name, *, dbt=False):
        return BashOperator(
            task_id=name,
            bash_command='exec "$MOVIES_REPO/.venv/bin/python" -m pipeline.steps ' + name,
            cwd=MOVIES_REPO,
            env={'MOVIES_REPO': MOVIES_REPO, 'PYTHONUNBUFFERED': '1'},
            append_env=True,
            do_xcom_push=False,
            skip_on_exit_code=None,
            trigger_rule='all_success',
            retries=2 if dbt else 0,
            retry_delay=timedelta(minutes=1),
            retry_exponential_backoff=True,
            execution_timeout=timedelta(minutes=30 if dbt else 10),
        )

    ingest = stage('ingest')
    dbt_prepare = stage('dbt_prepare', dbt=True)
    enrich_omdb = stage('enrich_omdb')
    dbt_build = stage('dbt_build', dbt=True)
    validate = stage('validate')

    ingest >> dbt_prepare >> enrich_omdb >> dbt_build >> validate
