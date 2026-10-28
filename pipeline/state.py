"""Versioned migration and stable operational keys, independent of dbt marts."""

from __future__ import annotations

from pathlib import Path

import duckdb

from pipeline import ROOT

VERSION = 1


def migrate(con) -> None:
    """Upgrade saved state once, atomically; never reconstruct it using the API."""
    con.execute('BEGIN TRANSACTION')
    try:
        con.execute('''CREATE TABLE IF NOT EXISTS pipeline_migrations (
            version INTEGER PRIMARY KEY, applied_at_utc TIMESTAMP NOT NULL)''')
        versions = [r[0] for r in con.execute('SELECT version FROM pipeline_migrations').fetchall()]
        if versions and max(versions) > VERSION:
            raise RuntimeError('Database uses a newer pipeline migration')
        if VERSION not in versions:
            pre_dbt = con.execute("""SELECT count(*) FROM information_schema.tables
                WHERE table_schema='main' AND table_name='dim_movie'""").fetchone()[0]
            con.execute((ROOT / 'sql/operational.sql').read_text(encoding='utf-8'))
            if pre_dbt:
                # Add missing cache columns before copying or detaching foreign keys.
                bootstrap_sql = (ROOT / 'sql/omdb.sql').read_text(encoding='utf-8')
                con.execute(bootstrap_sql.replace('REFERENCES movie_registry(movie_key)',
                                               'REFERENCES dim_movie(movie_key)'))
                con.execute('''INSERT INTO movie_registry
                    SELECT movie_key, source_title, run_start_date, run_end_date, display_title
                    FROM dim_movie''')
                con.execute('''INSERT INTO movie_metadata
                    SELECT movie_key, imdb_id, omdb_title, release_year, primary_genre,
                           imdb_rating, runtime_minutes, match_status FROM dim_movie''')
                con.execute('INSERT INTO distributor_registry SELECT * FROM dim_distributor')
                con.execute('INSERT INTO date_registry SELECT calendar_date FROM dim_date')
                con.execute('INSERT INTO revenue_assignments SELECT source_id, movie_key FROM fact_daily_revenue')
                # Some local databases contain review/search extensions beyond
                # the checked-in schema. Preserve them and detach their FKs too.
                dependents = con.execute("""SELECT DISTINCT t.table_name, t.sql
                    FROM duckdb_tables() t JOIN duckdb_constraints() c
                      ON t.database_name=c.database_name AND t.schema_name=c.schema_name
                     AND t.table_name=c.table_name
                    WHERE t.schema_name='main' AND c.constraint_type='FOREIGN KEY'
                      AND c.referenced_table='dim_movie'
                      AND t.table_name NOT IN ('fact_daily_revenue','omdb_lookup')
                    ORDER BY t.table_name""").fetchall()
                for index, (name, _) in enumerate(dependents):
                    quoted = '"' + name.replace('"', '""') + '"'
                    con.execute(f'CREATE TEMP TABLE migration_saved_{index} AS SELECT * FROM {quoted}')
                    con.execute(f'DROP TABLE {quoted}')
                con.execute('CREATE TEMP TABLE saved_lookup AS SELECT * FROM omdb_lookup')
                con.execute('DROP TABLE omdb_lookup')
                con.execute('DROP TABLE fact_daily_revenue')
                con.execute('DROP TABLE dim_movie')
                con.execute('DROP TABLE dim_distributor')
                con.execute('DROP TABLE dim_date')
                con.execute((ROOT / 'sql/omdb.sql').read_text(encoding='utf-8'))
                con.execute('INSERT INTO omdb_lookup SELECT * FROM saved_lookup')
                con.execute('DROP TABLE saved_lookup')
                for index, (name, ddl) in enumerate(dependents):
                    quoted = '"' + name.replace('"', '""') + '"'
                    con.execute(ddl.replace('REFERENCES dim_movie(movie_key)',
                                            'REFERENCES movie_registry(movie_key)'))
                    con.execute(f'INSERT INTO {quoted} SELECT * FROM migration_saved_{index}')
                    con.execute(f'DROP TABLE migration_saved_{index}')
            for table, key in [('movie_registry', 'movie_key'),
                               ('distributor_registry', 'distributor_key')]:
                next_key = con.execute(f'SELECT coalesce(max({key}),0)+1 FROM {table}').fetchone()[0]
                con.execute(f'CREATE SEQUENCE registry_{key}_seq START {next_key}')
            con.execute('INSERT INTO pipeline_migrations VALUES (?, timezone(\'UTC\',current_timestamp))', [VERSION])
        # Idempotent upgrades for old cache layouts, without modifying selected state.
        con.execute((ROOT / 'sql/omdb.sql').read_text(encoding='utf-8'))
        con.execute('COMMIT')
    except BaseException:
        con.execute('ROLLBACK')
        raise


def synchronize(con) -> None:
    """Validate old assignments before allocating any new keys or publishing."""
    con.execute('BEGIN TRANSACTION')
    try:
        changed = con.execute('''
            SELECT r.source_title FROM revenue_assignments a
            JOIN movie_registry r USING (movie_key)
            JOIN movie_metadata m USING (movie_key)
            JOIN intermediate.int_revenue_periods p USING (source_id)
            WHERE m.match_status <> 'pending'
              AND (r.source_title <> p.source_title OR r.run_start_date <> p.run_start_date)
            LIMIT 1
        ''').fetchone()
        if changed:
            raise ValueError(f'Reporting period changed for enriched title: {changed[0]}')
        con.execute('''INSERT INTO date_registry
            SELECT DISTINCT revenue_date FROM raw_revenues
            ON CONFLICT DO NOTHING''')
        con.execute('''INSERT INTO distributor_registry
            SELECT nextval('registry_distributor_key_seq'), distributor_name
            FROM (
                SELECT DISTINCT distributor_name FROM raw_revenues s
                WHERE NOT EXISTS (SELECT 1 FROM distributor_registry d
                                  WHERE d.distributor_name=s.distributor_name)
                ORDER BY distributor_name
            ) new_distributors''')
        con.execute('''INSERT INTO movie_registry
            SELECT nextval('registry_movie_key_seq'), source_title, run_start_date,
                   run_end_date, display_title
            FROM (
                SELECT * FROM intermediate.int_reporting_periods s
                WHERE NOT EXISTS (SELECT 1 FROM movie_registry m
                    WHERE m.source_title=s.source_title AND m.run_start_date=s.run_start_date)
                ORDER BY source_title, run_start_date
            ) new_periods''')
        con.execute('''UPDATE movie_registry m SET run_end_date=s.run_end_date,
                          display_title=s.display_title
            FROM intermediate.int_reporting_periods s
            WHERE m.source_title=s.source_title AND m.run_start_date=s.run_start_date''')
        con.execute('''INSERT INTO movie_metadata (movie_key)
            SELECT movie_key FROM movie_registry
            WHERE movie_key NOT IN (SELECT movie_key FROM movie_metadata)''')
        con.execute('DELETE FROM revenue_assignments')
        con.execute('''INSERT INTO revenue_assignments
            SELECT p.source_id, m.movie_key FROM intermediate.int_revenue_periods p
            JOIN movie_registry m USING (source_title, run_start_date)''')
        con.execute('COMMIT')
    except BaseException:
        con.execute('ROLLBACK')
        raise


def summary(db_path: Path) -> dict:
    with duckdb.connect(str(db_path), read_only=True) as con:
        row = con.execute('''SELECT count(*), count(DISTINCT movie_key),
            count(DISTINCT date_key), count(DISTINCT distributor_key),
            min(revenue_date), max(revenue_date), sum(f.revenue),
            count(*) FILTER (WHERE f.theaters IS NULL)
            FROM fact_daily_revenue f JOIN raw_revenues USING (source_id)''').fetchone()
    names = ('fact_rows', 'periods', 'dates', 'distributors', 'first_date',
             'last_date', 'total_revenue', 'missing_theaters')
    result = dict(zip(names, row))
    for key in ('first_date', 'last_date'):
        result[key] = str(result[key])
    return result
