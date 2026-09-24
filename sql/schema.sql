CREATE SEQUENCE IF NOT EXISTS movie_key_seq START 1;
CREATE SEQUENCE IF NOT EXISTS distributor_key_seq START 1;

CREATE TABLE IF NOT EXISTS raw_revenues (
    source_id VARCHAR PRIMARY KEY,
    revenue_date DATE NOT NULL,
    source_title VARCHAR NOT NULL,
    revenue BIGINT NOT NULL,
    theaters INTEGER,
    distributor_name VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key INTEGER PRIMARY KEY,
    calendar_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

-- One source title can have several reporting periods.
CREATE TABLE IF NOT EXISTS dim_movie (
    movie_key BIGINT PRIMARY KEY,
    source_title VARCHAR NOT NULL,
    run_start_date DATE NOT NULL,
    run_end_date DATE NOT NULL,
    display_title VARCHAR NOT NULL,
    imdb_id VARCHAR,
    omdb_title VARCHAR,
    release_year INTEGER,
    primary_genre VARCHAR,
    imdb_rating DOUBLE,
    runtime_minutes INTEGER,
    match_status VARCHAR NOT NULL DEFAULT 'pending',
    UNIQUE (source_title, run_start_date)
);

CREATE TABLE IF NOT EXISTS dim_distributor (
    distributor_key BIGINT PRIMARY KEY,
    distributor_name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_daily_revenue (
    source_id VARCHAR PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    movie_key BIGINT NOT NULL REFERENCES dim_movie(movie_key),
    distributor_key BIGINT NOT NULL REFERENCES dim_distributor(distributor_key),
    revenue BIGINT NOT NULL,
    theaters INTEGER
);
