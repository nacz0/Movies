{{ config(tags=['marts']) }}
select cast(r.movie_key as bigint) as movie_key,
       cast(r.source_title as varchar) as source_title,
       cast(r.run_start_date as date) as run_start_date,
       cast(r.run_end_date as date) as run_end_date,
       cast(r.display_title as varchar) as display_title,
       cast(m.imdb_id as varchar) as imdb_id,
       cast(m.omdb_title as varchar) as omdb_title,
       cast(m.release_year as integer) as release_year,
       cast(m.primary_genre as varchar) as primary_genre,
       cast(m.imdb_rating as double) as imdb_rating,
       cast(m.runtime_minutes as integer) as runtime_minutes,
       cast(m.match_status as varchar) as match_status
from {{ source('operational', 'movie_registry') }} r
join {{ ref('stg_omdb_lookup') }} m using (movie_key)
