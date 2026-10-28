-- Reconcile at source title/distributor/date granularity, not only a global sum.
with source_totals as (
    select source_title, distributor_name, revenue_date, sum(revenue) as revenue, count(*) as n
    from {{ ref('stg_revenues') }} group by 1, 2, 3
), fact_totals as (
    select m.source_title, x.distributor_name, d.calendar_date as revenue_date,
           sum(f.revenue) as revenue, count(*) as n
    from {{ ref('fact_daily_revenue') }} f
    join {{ ref('dim_movie') }} m using (movie_key)
    join {{ ref('dim_date') }} d using (date_key)
    join {{ ref('dim_distributor') }} x using (distributor_key)
    group by 1, 2, 3
)
select source_title, distributor_name, revenue_date
from source_totals s full outer join fact_totals f using (source_title, distributor_name, revenue_date)
where s.revenue is distinct from f.revenue or s.n is distinct from f.n
