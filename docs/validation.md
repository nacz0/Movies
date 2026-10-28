# Executed validation

Run locally on Windows, 28 October 2026, using Python 3.12.8, DuckDB 1.5.1, dbt Core 1.11.16 and dbt-duckdb 1.11.1. All test databases were new or temporary files. Existing user databases were not used as build targets.

## Results

| Check | Result |
| --- | --- |
| Full Python suite | **24 passed** in 171.341 seconds on the final run |
| Full-source acceptance | Two complete imports/builds; every row of all four marts identical on the second run |
| Fact rows | **337,818** |
| Total gross | **$205,110,995,141** |
| Reporting periods | **6,661** |
| Source titles / distributors / dates | **6,545 / 363 / 8,455** |
| Missing theaters | **161**, retained as NULL |
| dbt build | **11 models**, **41 data tests**, **2 SQL unit tests**, all successful |
| Explicit `dbt test` | **43 passed**, no errors, warnings or skipped tests |
| Dashboard | All query functions/sort options; local HTTP metadata/dashboard/filter routes; invalid sort rejected |
| CSV exports | Both existing JavaScript export functions executed with Node 24; filenames, headers, quote escaping, nulls and USD mapping verified |
| Network safety | No real OMDb calls; fake fetchers and a URL-opening guard in integration/full-source verification |
| Failure safety | Invalid imports preserve raw rows; interrupted requests retain reservations/cache; dbt failures stop the runner; rerun repairs marts |
| Dependency consistency | `python -m pip check`: no broken requirements |
| Workflow syntax | Old parsing error reproduced; new workflow parses successfully |

Commands executed (using `.venv/Scripts/python.exe` locally):

```powershell
python -m unittest discover -s tests -v
python -m tests.verify_full --db data/movies.duckdb
python -m pipeline.run --offline
python -m pip check
```

Explicit dbt tests used the small `data/simplified-check.duckdb` file, with `MOVIES_DBT_PATH` set to its absolute path:

```powershell
dbt test --project-dir dbt --profiles-dir dbt
```

The full-source script blocks URL opening, asserts the expected fact count/gross, compares every mart row across two imports, checks zero request reservations and exercises dashboard totals, trends and rankings. SQL row/group reconciliation and relationship tests execute during each build. The resulting ignored `data/movies.duckdb` is ready for the dashboard, with 6,661 pending periods because enrichment was offline.

Integration negative controls deliberately introduced negative revenue, zero theaters and an invalid movie key. The real `dbt test` returned failure for row/gross reconciliation, valid values and movie relationships. A separate simulated final-build failure verified cache/budget durability and successful repair on rerun. HTTP tests cover timeout, invalid JSON, invalid payloads, 401/403/429/503 errors and interrupted requests.

## CI correction and limits

The old `dbt` branch workflow has this plain YAML value:

```yaml
- run: python -m pip install --only-binary=:all: -r requirements.txt
```

The final colon followed by a space causes `mapping values are not allowed here`. This was reproduced by parsing the exact file from the branch. The replacement quotes the complete command, removes the migration/publication benchmark dependency and runs the simple pipeline against an isolated fixture. Windows and Linux jobs need no API secrets.

The hosted matrix and a native Linux run have **not** been executed from this local branch. Historical run logs/statuses were unavailable through the connected GitHub tools. Local reproduction establishes the YAML defect; it does not establish whether older hosted jobs had additional problems.

A clean dependency-resolution dry run could not be completed: package metadata retrieval stalled after the `agate` metadata even with short timeouts, retries disabled and noninteractive mode. It was canceled without installing anything. Tests used the already-installed exact pinned runtime versions; `pip check` passed. A clean hosted installation remains a CI verification item.

## Complexity comparison

Counts include blank lines/docstrings and `pipeline/__init__.py`; generated artifacts, tests and dashboard code are excluded from Python LOC.

| Measure | Existing `dbt` branch | `simplified-dbt` |
| --- | ---: | ---: |
| Pipeline Python LOC | 850 | 414 |
| Functional pipeline modules (excluding `__init__`) | 5 | 3 |
| Python-owned durable tables (including raw source) | 11 | 4 |
| Custom dbt macro files | 2 | 0 |
| SQL analytical models | 11 | 11 |

The new code omits key registries, migration tracking, import history, durable source-to-period synchronization, working/backup database publication, resume and restore orchestration. Full rebuilds and deterministic keys replace those mechanisms. Essential cache/reservation durability stays in place. The complete dbt build is not atomic; keep the dashboard stopped after failure until a successful rerun.

## File changes relative to `main`

- Created `pipeline/ingest.py`, `pipeline/enrich.py` and `pipeline/run.py`.
- Created `dbt/dbt_project.yml`, `dbt/profiles.yml`, source/model documentation, two staging models, five intermediate models, four mart models and four custom SQL data tests. Two SQL unit tests are defined in `dbt/models/models.yml`.
- Created `tests/test_ingest.py`, `tests/test_enrich.py`, `tests/test_dbt.py`, `tests/verify_full.py`, `tests/__init__.py` and `tests/fixtures/revenues.csv`.
- Created `.github/workflows/verify.yml` and this validation record.
- Changed `README.md`, `.gitignore`, `requirements.txt`, `pipeline/__init__.py` and the dashboard server's default database filename.
- Removed `pipeline/load_revenues.py`, `pipeline/enrich_omdb.py`, `sql/schema.sql`, `sql/omdb.sql` and the replaced `tests/test_pipeline.py`.
- Kept dashboard queries/HTML, the complete CSV, `main`/`dbt` branch tips and existing local databases intact.
