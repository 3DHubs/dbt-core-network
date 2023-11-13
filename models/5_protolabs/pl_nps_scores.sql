select rating,
       order_id as order_uuid,
       submit_date as submit_at
from {{ source('data_lake', 'nps_scores') }}
where submit_date >= date_add('years',-2,date_trunc('year',getdate()))
