select source_id, revenue_date, source_title, revenue, theaters, distributor_name
from {{ source('python', 'raw_revenues') }}
