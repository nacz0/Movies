-- Existing responses stay available when the CSV is reloaded.
CREATE TABLE IF NOT EXISTS omdb_lookup (
    movie_key BIGINT PRIMARY KEY REFERENCES dim_movie(movie_key),
    lookup_status VARCHAR NOT NULL,
    candidate_imdb_id VARCHAR,
    candidate_title VARCHAR,
    match_reason VARCHAR,
    response_json VARCHAR,
    fetched_at_utc TIMESTAMP NOT NULL
);

-- Zero denotes a title-only query; actual years identify year-specific queries.
CREATE TABLE IF NOT EXISTS omdb_query_cache (
    source_title VARCHAR NOT NULL,
    query_year INTEGER NOT NULL,
    response_json VARCHAR NOT NULL,
    fetched_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (source_title, query_year)
);

ALTER TABLE omdb_lookup ADD COLUMN IF NOT EXISTS query_year INTEGER;

-- Reserve each request before sending it so a failed call still uses budget.
CREATE TABLE IF NOT EXISTS omdb_request_log (
    request_id VARCHAR PRIMARY KEY,
    movie_key BIGINT NOT NULL,
    requested_at_utc TIMESTAMP NOT NULL,
    request_status VARCHAR NOT NULL,
    http_status INTEGER,
    request_kind VARCHAR NOT NULL DEFAULT 'title'
);
