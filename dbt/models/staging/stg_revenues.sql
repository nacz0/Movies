{{ config(tags=['periods']) }}
select cast(source_id as varchar) as source_id,
       cast(revenue_date as date) as revenue_date,
       cast(source_title as varchar) as source_title,
       cast(revenue as bigint) as revenue,
       cast(theaters as integer) as theaters,
       cast(distributor_name as varchar) as distributor_name
from {{ source('operational', 'raw_revenues') }}
