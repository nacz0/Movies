{{ config(tags=['periods']) }}
select source_id from {{ ref('stg_revenues') }}
except all select source_id from {{ ref('int_revenue_periods') }}
