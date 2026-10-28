select s.*, p.movie_key
from {{ ref('stg_revenues') }} s
join {{ ref('int_reporting_period_dates') }} d using (source_title, revenue_date)
join {{ ref('int_reporting_periods') }} p using (source_title, run_number)
