select md5(distributor_name) as distributor_key, distributor_name
from (select distinct distributor_name from {{ ref('stg_revenues') }}) distributors
