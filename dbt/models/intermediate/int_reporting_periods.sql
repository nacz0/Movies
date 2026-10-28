with runs as (
    select source_title, run_number, min(revenue_date) as run_start_date,
           max(revenue_date) as run_end_date
    from {{ ref('int_reporting_period_dates') }} group by 1, 2
)
select md5(to_json([source_title, cast(run_start_date as varchar)])) as movie_key,
       *, case when count(*) over (partition by source_title) > 1
               then source_title || ' (' || cast(run_start_date as varchar) || ')'
               else source_title end as display_title
from runs
