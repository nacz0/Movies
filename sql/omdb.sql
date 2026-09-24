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

-- Reserve each request before sending it so a failed call still uses budget.
CREATE TABLE IF NOT EXISTS omdb_request_log (
    request_id VARCHAR PRIMARY KEY,
    movie_key BIGINT NOT NULL,
    requested_at_utc TIMESTAMP NOT NULL,
    request_status VARCHAR NOT NULL,
    http_status INTEGER,
    request_kind VARCHAR NOT NULL DEFAULT 'title'
);
