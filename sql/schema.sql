CREATE SEQUENCE IF NOT EXISTS movie_key_seq START 1;
CREATE SEQUENCE IF NOT EXISTS film_key_seq START 1;
CREATE SEQUENCE IF NOT EXISTS distributor_key_seq START 1;

CREATE TABLE IF NOT EXISTS raw_revenues (
    source_id VARCHAR PRIMARY KEY,
    revenue_date DATE NOT NULL,
    source_title VARCHAR NOT NULL,
    revenue BIGINT NOT NULL CHECK (revenue >= 0),
    theaters INTEGER CHECK (theaters > 0),
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

-- One provisional theatrical run per title and period separated by >180 idle days.
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

-- Current reporting dates are mutable; movie_key and its original anchor are not.
CREATE TABLE IF NOT EXISTS dim_release_run (
    movie_key BIGINT PRIMARY KEY REFERENCES dim_movie(movie_key),
    run_start_date DATE NOT NULL,
    run_end_date DATE NOT NULL,
    display_title VARCHAR NOT NULL
);

-- IMDb ID identifies a production; several revenue periods may point to it.
CREATE TABLE IF NOT EXISTS dim_film (
    film_key BIGINT PRIMARY KEY,
    imdb_id VARCHAR NOT NULL UNIQUE
);

-- Descriptive attributes can be refreshed without changing the film identity.
CREATE TABLE IF NOT EXISTS film_details (
    film_key BIGINT PRIMARY KEY REFERENCES dim_film(film_key),
    film_title VARCHAR NOT NULL,
    release_year INTEGER,
    primary_genre VARCHAR,
    imdb_rating DOUBLE,
    runtime_minutes INTEGER
);

CREATE OR REPLACE VIEW v_run_film_match AS
SELECT m.movie_key, f.film_key
FROM dim_movie m
JOIN dim_release_run r ON r.movie_key = m.movie_key
JOIN dim_film f ON f.imdb_id = m.imdb_id
WHERE m.match_status IN ('matched', 'verified_id');

-- Keep an audit trail when a reload changes the dates of an existing run.
CREATE TABLE IF NOT EXISTS run_boundary_change_log (
    change_id VARCHAR PRIMARY KEY,
    movie_key BIGINT NOT NULL REFERENCES dim_movie(movie_key),
    old_start_date DATE NOT NULL,
    old_end_date DATE NOT NULL,
    new_start_date DATE NOT NULL,
    new_end_date DATE NOT NULL,
    previous_match_status VARCHAR NOT NULL,
    changed_at_utc TIMESTAMP NOT NULL
);

-- Exact, reviewer-approved exceptions to the automatic 180-day boundary.
CREATE TABLE IF NOT EXISTS title_boundary_rule (
    source_title VARCHAR NOT NULL,
    boundary_date DATE NOT NULL,
    action VARCHAR NOT NULL CHECK (action IN ('split', 'join')),
    reason VARCHAR NOT NULL CHECK (length(trim(reason)) > 0),
    reviewed_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (source_title, boundary_date)
);

CREATE TABLE IF NOT EXISTS title_boundary_rule_event (
    event_id VARCHAR PRIMARY KEY,
    source_title VARCHAR NOT NULL,
    boundary_date DATE NOT NULL,
    previous_action VARCHAR,
    new_action VARCHAR,
    reason VARCHAR NOT NULL,
    changed_at_utc TIMESTAMP NOT NULL
);

-- Upgrade databases created before the enrichment fields were added.
ALTER TABLE dim_movie ADD COLUMN IF NOT EXISTS imdb_rating DOUBLE;
ALTER TABLE dim_movie ADD COLUMN IF NOT EXISTS runtime_minutes INTEGER;

CREATE TABLE IF NOT EXISTS dim_distributor (
    distributor_key BIGINT PRIMARY KEY,
    distributor_name VARCHAR NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_daily_revenue (
    source_id VARCHAR PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    movie_key BIGINT NOT NULL REFERENCES dim_movie(movie_key),
    distributor_key BIGINT NOT NULL REFERENCES dim_distributor(distributor_key),
    revenue BIGINT NOT NULL CHECK (revenue >= 0),
    theaters INTEGER CHECK (theaters > 0)
);
