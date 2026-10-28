{{ config(tags=['marts']) }}
select cast(distributor_key as bigint) as distributor_key,
       cast(distributor_name as varchar) as distributor_name
from {{ source('operational', 'distributor_registry') }}
