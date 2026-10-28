{{ config(tags=['marts']) }}
with actual as (
    select d.calendar_date as revenue_date, m.source_title, x.distributor_name,
           f.revenue, f.theaters
    from {{ ref('fact_daily_revenue') }} f
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_movie') }} m using (movie_key)
    join {{ ref('dim_distributor') }} x using (distributor_key)
), expected as (select * from {{ ref('stg_revenues') }}), totals as (
    {% for relation in ['actual', 'expected'] %}
    select '{{ relation }}' as origin, revenue_date, source_title, distributor_name,
           grouping_id(revenue_date, source_title, distributor_name) as grain,
           count(*) as rows, sum(revenue) as revenue,
           count(*) filter (where theaters is null) as missing_theaters
    from {{ relation }}
    group by grouping sets ((), (revenue_date), (source_title), (distributor_name))
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
)
(select * exclude(origin) from totals where origin = 'actual'
 except all select * exclude(origin) from totals where origin = 'expected')
union all
(select * exclude(origin) from totals where origin = 'expected'
 except all select * exclude(origin) from totals where origin = 'actual')
