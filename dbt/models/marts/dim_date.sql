{{ config(tags=['marts']) }}
select cast(strftime(calendar_date, '%Y%m%d') as integer) as date_key,
       cast(calendar_date as date) as calendar_date,
       cast(year(calendar_date) as integer) as year,
       cast(quarter(calendar_date) as integer) as quarter,
       cast(month(calendar_date) as integer) as month,
       cast(isodow(calendar_date) as integer) as day_of_week
from {{ source('operational', 'date_registry') }}
