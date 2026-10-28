select s.source_id, d.date_key, s.movie_key, x.distributor_key, s.revenue, s.theaters
from {{ ref('int_revenue_periods') }} s
join {{ ref('dim_movie') }} m using (movie_key)
join {{ ref('dim_date') }} d on d.calendar_date = s.revenue_date
join {{ ref('dim_distributor') }} x using (distributor_name)
