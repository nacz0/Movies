{{ config(tags=['candidates']) }}
select r.movie_key, p.source_title, year(p.run_start_date) as first_year,
       year(p.run_end_date) as last_year, m.match_status, sum(f.revenue) as revenue
from {{ ref('int_reporting_periods') }} p
join {{ source('operational', 'movie_registry') }} r using (source_title, run_start_date)
join {{ source('operational', 'movie_metadata') }} m using (movie_key)
join {{ ref('int_revenue_periods') }} f using (source_title, run_start_date)
group by 1, 2, 3, 4, 5
