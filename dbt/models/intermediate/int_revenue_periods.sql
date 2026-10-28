{{ config(tags=['periods'], materialized='table') }}
select s.*, p.run_start_date
from {{ ref('stg_revenues') }} s
join {{ ref('int_reporting_period_dates') }} d using (source_title, revenue_date)
join {{ ref('int_reporting_periods') }} p using (source_title, run_number)
