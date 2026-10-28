select p.movie_key, p.source_title, p.run_start_date, p.run_end_date, sum(f.revenue) as revenue
from {{ ref('int_reporting_periods') }} p
join {{ ref('int_revenue_periods') }} f using (movie_key)
group by 1, 2, 3, 4
