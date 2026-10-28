select cast(strftime(revenue_date, '%Y%m%d') as integer) as date_key,
       revenue_date as calendar_date, cast(year(revenue_date) as integer) as year,
       cast(quarter(revenue_date) as integer) as quarter,
       cast(month(revenue_date) as integer) as month,
       cast(isodow(revenue_date) as integer) as day_of_week
from (select distinct revenue_date from {{ ref('stg_revenues') }}) dates
