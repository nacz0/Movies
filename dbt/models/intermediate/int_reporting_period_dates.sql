{{ config(tags=['periods']) }}
select source_title, revenue_date,
       sum(case when previous_date is null
                     or date_diff('day', previous_date, revenue_date) > 180
                then 1 else 0 end)
           over (partition by source_title order by revenue_date) as run_number
from {{ ref('int_title_dates') }}
