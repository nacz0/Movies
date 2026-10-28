{{ config(tags=['marts']) }}
select m.movie_key, m.imdb_id, m.omdb_title, m.release_year, m.primary_genre,
       m.imdb_rating, m.runtime_minutes, m.match_status,
       l.lookup_status, l.match_reason, l.query_year, l.fetched_at_utc
from {{ source('operational', 'movie_metadata') }} m
left join {{ source('operational', 'omdb_lookup') }} l using (movie_key)
