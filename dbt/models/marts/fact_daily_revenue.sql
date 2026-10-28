{{ config(tags=['marts']) }}
select cast(s.source_id as varchar) as source_id, d.date_key, m.movie_key,
       x.distributor_key, cast(s.revenue as bigint) as revenue,
       cast(s.theaters as integer) as theaters
from {{ ref('int_revenue_periods') }} s
join {{ ref('dim_movie') }} m using (source_title, run_start_date)
join {{ ref('dim_date') }} d on d.calendar_date = s.revenue_date
join {{ ref('dim_distributor') }} x using (distributor_name)
