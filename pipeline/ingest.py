"""Own four durable tables and replace a validated CSV snapshot atomically."""

from pathlib import Path

import duckdb


def initialize(db_path: Path) -> None:
    db_path = Path(db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    # Inspect existing files read-only before executing any DDL.
    if db_path.exists():
        with duckdb.connect(str(db_path), read_only=True) as con:
            tables = set(con.execute("""select table_schema, table_name
                from information_schema.tables""").fetchall())
            owned = {('movies_state', name) for name in
                     ('raw_revenues', 'omdb_cache', 'omdb_requests', 'omdb_matches')}
            if tables and not owned.issubset(tables):
                raise ValueError('Existing database uses another format. Choose a NEW --db path; '
                                 'optionally use --reuse-cache-from to copy reusable OMDb data.')
    with duckdb.connect(str(db_path)) as con:
        con.execute("""
            BEGIN;
            CREATE SCHEMA IF NOT EXISTS movies_state;
            CREATE TABLE IF NOT EXISTS movies_state.raw_revenues (
                source_id VARCHAR PRIMARY KEY, revenue_date DATE NOT NULL,
                source_title VARCHAR NOT NULL, revenue BIGINT NOT NULL,
                theaters INTEGER, distributor_name VARCHAR NOT NULL);
            CREATE TABLE IF NOT EXISTS movies_state.omdb_cache (
                query_title VARCHAR, query_year INTEGER, response_json VARCHAR NOT NULL,
                fetched_at TIMESTAMPTZ NOT NULL, PRIMARY KEY (query_title, query_year));
            CREATE TABLE IF NOT EXISTS movies_state.omdb_requests (
                request_id VARCHAR PRIMARY KEY, requested_at TIMESTAMPTZ NOT NULL,
                query_title VARCHAR NOT NULL, query_year INTEGER NOT NULL,
                outcome VARCHAR NOT NULL);
            CREATE TABLE IF NOT EXISTS movies_state.omdb_matches (
                movie_key VARCHAR PRIMARY KEY, run_end_date DATE NOT NULL,
                match_status VARCHAR NOT NULL, match_reason VARCHAR NOT NULL,
                response_json VARCHAR);
            COMMIT;
        """)


def import_snapshot(csv_path: Path, db_path: Path) -> dict:
    csv_path = Path(csv_path).resolve()
    if not csv_path.is_file():
        raise FileNotFoundError(csv_path)
    initialize(db_path)
    with duckdb.connect(str(db_path)) as con:
        con.execute('BEGIN')
        try:
            # Read text first: integer/date casts must reject lossy numeric coercion.
            con.execute("""CREATE TEMP TABLE csv_input AS SELECT * FROM read_csv(
                ?, header=true, all_varchar=true, nullstr='', strict_mode=true)""",
                        [str(csv_path)])
            columns = [row[0] for row in con.execute('DESCRIBE csv_input').fetchall()]
            if columns != ['id', 'date', 'title', 'revenue', 'theaters', 'distributor']:
                raise ValueError('CSV header must be id,date,title,revenue,theaters,distributor')
            invalid = con.execute("""SELECT count(*) FROM csv_input
                WHERE id IS NULL OR trim(id) = '' OR title IS NULL OR trim(title) = ''
                   OR distributor IS NULL OR trim(distributor) = ''
                   OR NOT coalesce(regexp_full_match(revenue, '[0-9]+'), false)
                   OR try_cast(revenue AS BIGINT) IS NULL
                   OR NOT coalesce(regexp_full_match(date, '[0-9]{4}-[0-9]{2}-[0-9]{2}'), false)
                   OR try_cast(date AS DATE) IS NULL
                   OR (theaters IS NOT NULL AND
                       (NOT regexp_full_match(theaters, '[0-9]+')
                        OR try_cast(theaters AS INTEGER) IS NULL
                        OR try_cast(theaters AS INTEGER) <= 0))""").fetchone()[0]
            if invalid:
                raise ValueError(f'CSV has {invalid} invalid rows')
            con.execute("""CREATE TEMP TABLE staged AS
                SELECT id AS source_id, cast(date AS DATE) AS revenue_date,
                       title AS source_title, cast(revenue AS BIGINT) AS revenue,
                       cast(theaters AS INTEGER) AS theaters, distributor AS distributor_name
                FROM csv_input""")
            rows, ids, total = con.execute(
                'SELECT count(*), count(DISTINCT source_id), sum(revenue) FROM staged').fetchone()
            if not rows or rows != ids:
                raise ValueError('CSV is empty or has duplicate IDs')
            duplicate = con.execute("""SELECT 1 FROM staged
                GROUP BY revenue_date, source_title, distributor_name HAVING count(*) > 1
                LIMIT 1""").fetchone()
            if duplicate:
                raise ValueError('CSV has duplicate date/title/distributor records')
            con.execute('DELETE FROM movies_state.raw_revenues')
            con.execute('INSERT INTO movies_state.raw_revenues SELECT * FROM staged')
            con.execute('COMMIT')
        except BaseException:
            con.execute('ROLLBACK')
            raise
    return {'rows': rows, 'total_revenue': total}
