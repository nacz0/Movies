# Airflow integration validation

Executed on 2026-10-29 in WSL2 Ubuntu, on `simplified-dbt`, using the installed
Airflow 3.3.2 environment at `/home/nacho/airflow/.venv` and the separate Movies
environment at `/home/nacho/projects/Movies/.venv`. No dependencies were installed
or changed. The Windows checkout and WSL checkout contain the same integration files.

Commands executed from `/home/nacho/projects/Movies`:

```bash
.venv/bin/python -m pip check
.venv/bin/python -m unittest discover -s tests -v
bash -n scripts/install_airflow_dag.sh
bash scripts/install_airflow_dag.sh
/home/nacho/airflow/.venv/bin/python -m tests.verify_airflow
/home/nacho/airflow/.venv/bin/python -m tests.verify_airflow --run
/home/nacho/airflow/.venv/bin/python -m tests.verify_airflow --failure
/home/nacho/airflow/.venv/bin/airflow dags list --local
```

- Dependency check: no broken requirements.
- Python suite: **28 tests passed** in 100.885 seconds. These include real dbt
  builds/tests, intentional data corruption checks, HTTP fakes and stage tests.
- DAG parsing: passed in the installed Airflow environment. Verified task IDs,
  dependency edges, manual schedule, concurrency limits, retry/timeout settings,
  failure trigger rules, Movies interpreter commands and disabled XCom pushes.
- Full local Airflow run: all **five tasks succeeded**, including the final
  acceptance checks. This used `dag.test()` with real subprocesses and the existing
  Airflow metadata database, without the scheduler/executor. Run ID:
  `manual__2026-10-29T18:49:09.671603+00:00`.
- Full `dbt_build` artifact: **11 successful models and 43 passing tests**
  (41 data tests and two SQL unit tests), with no failed/skipped results.
- Dedicated output: `/home/nacho/projects/Movies/data/movies_steps.duckdb`.

```json
{"fact_rows": 337818, "total_revenue": 205110995141, "periods": 6661, "omdb_requests": 0}
```

Offline enrichment passes no key, a zero run limit and a zero daily cap to the
existing function. Its run summary and the final database both reported zero
requests/reservations. The stage integration test also set an environment API key
while intercepting `urllib.request.urlopen`; the interceptor was never called.

The negative control used a temporary Airflow home, SQLite metadata database and
DAG bundle. A copied DAG pointed ingest at a nonexistent CSV and temporary database.
Ingest failed with exit code 1; all four dependent tasks became `upstream_failed`
with zero execution attempts. The temporary Movies database was never created.
The production DAG and dataset were unaffected by this negative control.

Installation and a second, idempotent installer invocation succeeded. Local DAG
listing showed `movies_steps` alongside `hello_movies` and `movies_local`. The new
DAG remains paused until explicitly unpaused; no scheduled run was triggered.
Before/after SHA-256 checks confirmed that both existing DAG files and the existing
`data/movies_airflow.duckdb` were unchanged. Existing Windows databases were not used.
No keys, local databases, generated dbt artifacts or logs were added to Git.

The installed Airflow emitted warnings about its Starlette/httpx compatibility and
missing optional Graphviz rendering. Neither prevented DAG parsing or execution.
Retries and timeouts were checked as configuration; actual timeout/automatic retry
exhaustion was not induced. Scheduler-driven execution has not been tested.
