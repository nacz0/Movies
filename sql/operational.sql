-- Python owns these tables. dbt rebuilds may read, but never replace them.
CREATE TABLE IF NOT EXISTS raw_revenues (
    source_id VARCHAR PRIMARY KEY,
    revenue_date DATE NOT NULL,
    source_title VARCHAR NOT NULL,
    revenue BIGINT NOT NULL,
    theaters INTEGER,
    distributor_name VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS movie_registry (
    movie_key BIGINT PRIMARY KEY,
    source_title VARCHAR NOT NULL,
    run_start_date DATE NOT NULL,
    run_end_date DATE NOT NULL,
    display_title VARCHAR NOT NULL,
    UNIQUE (source_title, run_start_date)
);
CREATE TABLE IF NOT EXISTS distributor_registry (
    distributor_key BIGINT PRIMARY KEY,
    distributor_name VARCHAR NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS date_registry (calendar_date DATE PRIMARY KEY);
CREATE TABLE IF NOT EXISTS movie_metadata (
    movie_key BIGINT PRIMARY KEY REFERENCES movie_registry(movie_key),
    imdb_id VARCHAR,
    omdb_title VARCHAR,
    release_year INTEGER,
    primary_genre VARCHAR,
    imdb_rating DOUBLE,
    runtime_minutes INTEGER,
    match_status VARCHAR NOT NULL DEFAULT 'pending'
);
CREATE TABLE IF NOT EXISTS revenue_assignments (
    source_id VARCHAR PRIMARY KEY,
    movie_key BIGINT NOT NULL REFERENCES movie_registry(movie_key)
);
CREATE TABLE IF NOT EXISTS import_history (
    imported_at_utc TIMESTAMP NOT NULL,
    csv_sha256 VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    total_revenue HUGEINT NOT NULL,
    missing_theaters BIGINT NOT NULL
);
