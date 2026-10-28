select source_id from {{ ref('fact_daily_revenue') }}
where revenue < 0 or (theaters is not null and theaters <= 0)
