-- A full outer comparison catches missing, duplicate, corrected and misassigned rows.
select coalesce(r.source_id, f.source_id) as source_id
from {{ ref('stg_revenues') }} r
full outer join {{ ref('fact_daily_revenue') }} f using (source_id)
left join {{ ref('dim_date') }} d on d.date_key = f.date_key
left join {{ ref('dim_movie') }} m on m.movie_key = f.movie_key
left join {{ ref('dim_distributor') }} x on x.distributor_key = f.distributor_key
where r.source_id is null or f.source_id is null
   or r.revenue is distinct from f.revenue or r.theaters is distinct from f.theaters
   or r.revenue_date is distinct from d.calendar_date
   or r.source_title is distinct from m.source_title
   or r.distributor_name is distinct from x.distributor_name
   or r.revenue_date not between m.run_start_date and m.run_end_date
