{{ config(tags=['marts']) }}
with actual as (
    select f.source_id, d.calendar_date as revenue_date, m.source_title,
           f.revenue, f.theaters, x.distributor_name
    from {{ ref('fact_daily_revenue') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_movie') }} m using (movie_key)
    join {{ ref('dim_distributor') }} x using (distributor_key)
), expected as (select * from {{ ref('stg_revenues') }})
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
