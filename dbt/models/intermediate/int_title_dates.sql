{{ config(tags=['periods']) }}
select source_title, revenue_date,
       lag(revenue_date) over (partition by source_title order by revenue_date) as previous_date
from (select distinct source_title, revenue_date from {{ ref('stg_revenues') }}) dates
