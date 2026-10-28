select p.movie_key, p.source_title, p.run_start_date, p.run_end_date, p.display_title,
       case when m.match_status = 'matched' then m.imdb_id end as imdb_id,
       case when m.match_status = 'matched' then m.omdb_title end as omdb_title,
       case when m.match_status = 'matched' then m.release_year end as release_year,
       case when m.match_status = 'matched' then m.primary_genre end as primary_genre,
       case when m.match_status = 'matched' then m.imdb_rating end as imdb_rating,
       case when m.match_status = 'matched' then m.runtime_minutes end as runtime_minutes,
       coalesce(m.match_status, 'pending') as match_status
from {{ ref('int_reporting_periods') }} p
left join {{ ref('stg_omdb_matches') }} m
    on p.movie_key = m.movie_key and p.run_end_date = m.run_end_date
