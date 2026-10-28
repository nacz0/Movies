select source_id from {{ ref('int_revenue_periods') }}
group by source_id having count(*) <> 1
