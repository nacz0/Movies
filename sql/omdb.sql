-- A reservation is committed before each HTTP request. It counts even if the
-- process crashes or the network fails, so the request budget is conservative.
CREATE TABLE IF NOT EXISTS omdb_request_log (
    request_id VARCHAR PRIMARY KEY,
    movie_key BIGINT NOT NULL,
    requested_at_utc TIMESTAMP NOT NULL,
    request_status VARCHAR NOT NULL,
    http_status INTEGER,
    request_kind VARCHAR NOT NULL DEFAULT 'title'
);
ALTER TABLE omdb_request_log ADD COLUMN IF NOT EXISTS request_kind VARCHAR DEFAULT 'title';

-- Aliases change only the OMDb query, never the CSV title or revenue grouping.
CREATE TABLE IF NOT EXISTS title_search_alias (
    source_title VARCHAR NOT NULL,
    search_title VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    created_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (source_title, search_title)
);

CREATE TABLE IF NOT EXISTS omdb_search_cache (
    movie_key BIGINT NOT NULL REFERENCES dim_movie(movie_key),
    search_title VARCHAR NOT NULL,
    year_hint INTEGER NOT NULL,
    response_json VARCHAR NOT NULL,
    fetched_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (movie_key, search_title, year_hint)
);

CREATE TABLE IF NOT EXISTS omdb_candidate (
    movie_key BIGINT NOT NULL REFERENCES dim_movie(movie_key),
    imdb_id VARCHAR NOT NULL,
    candidate_title VARCHAR NOT NULL,
    candidate_year VARCHAR,
    search_title VARCHAR,
    year_hint INTEGER,
    detail_json VARCHAR,
    discovered_at_utc TIMESTAMP NOT NULL,
    PRIMARY KEY (movie_key, imdb_id)
);

CREATE TABLE IF NOT EXISTS omdb_lookup (
    movie_key BIGINT PRIMARY KEY REFERENCES dim_movie(movie_key),
    lookup_status VARCHAR NOT NULL,
    candidate_imdb_id VARCHAR,
    candidate_title VARCHAR,
    match_reason VARCHAR,
    response_json VARCHAR,
    fetched_at_utc TIMESTAMP NOT NULL
);

-- One current human decision per theatrical run. The original OMDb response
-- stays in omdb_lookup so an override can be audited or undone.
CREATE TABLE IF NOT EXISTS movie_match_decision (
    movie_key BIGINT PRIMARY KEY REFERENCES dim_movie(movie_key),
    decision VARCHAR NOT NULL CHECK (decision IN
        ('accept_candidate', 'reject_candidate', 'verify_id')),
    imdb_id VARCHAR,
    reason VARCHAR NOT NULL CHECK (length(trim(reason)) > 0),
    reviewed_at_utc TIMESTAMP NOT NULL
);

CREATE OR REPLACE VIEW v_movie_match_review AS
WITH active_runs AS (
    SELECT m.movie_key, m.source_title, r.display_title,
           r.run_start_date, r.run_end_date, m.match_status,
           SUM(f.revenue) AS total_revenue,
           COUNT(*) AS reported_rows
    FROM dim_movie m
    JOIN dim_release_run r ON r.movie_key = m.movie_key
    JOIN fact_daily_revenue f ON f.movie_key = m.movie_key
    GROUP BY m.movie_key, m.source_title, r.display_title,
             r.run_start_date, r.run_end_date, m.match_status
), titled AS (
    SELECT *, COUNT(*) OVER (PARTITION BY source_title) AS title_run_count
    FROM active_runs
), boundary_changes AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY movie_key ORDER BY changed_at_utc DESC, change_id DESC
    ) AS recency
    FROM run_boundary_change_log
), alternatives AS (
    SELECT movie_key, COUNT(*) AS candidate_count,
           string_agg(candidate_title || ' (' || COALESCE(candidate_year, '?') ||
                      ') ' || imdb_id, '; ' ORDER BY candidate_title, imdb_id) AS candidate_summary
    FROM omdb_candidate GROUP BY movie_key
), manual_boundaries AS (
    SELECT source_title, COUNT(*) AS manual_boundary_count
    FROM title_boundary_rule GROUP BY source_title
)
SELECT m.movie_key, m.source_title, m.display_title,
       m.run_start_date, m.run_end_date, m.title_run_count,
       m.total_revenue, m.reported_rows, m.match_status,
       l.candidate_imdb_id, l.candidate_title,
       json_extract_string(l.response_json, '$.Year') AS candidate_year,
       l.match_reason, j.decision, j.imdb_id AS verified_imdb_id,
       j.reason AS review_reason, j.reviewed_at_utc,
       COALESCE(a.candidate_count, 0) AS alternative_candidate_count,
       a.candidate_summary AS alternative_candidates,
       COALESCE(mb.manual_boundary_count, 0) AS manual_boundary_count,
       bc.old_start_date AS previous_run_start_date,
       bc.old_end_date AS previous_run_end_date,
       bc.changed_at_utc AS boundary_changed_at_utc
FROM titled m
LEFT JOIN omdb_lookup l ON l.movie_key = m.movie_key
LEFT JOIN movie_match_decision j ON j.movie_key = m.movie_key
LEFT JOIN alternatives a ON a.movie_key = m.movie_key
LEFT JOIN manual_boundaries mb ON mb.source_title = m.source_title
LEFT JOIN boundary_changes bc ON bc.movie_key = m.movie_key AND bc.recency = 1
WHERE m.match_status IN ('ambiguous', 'not_found', 'rejected', 'verified_id')
   OR m.title_run_count > 1
   OR a.movie_key IS NOT NULL
   OR j.movie_key IS NOT NULL
   OR mb.source_title IS NOT NULL
   OR bc.movie_key IS NOT NULL;
