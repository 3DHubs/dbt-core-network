select rating,
       order_id as order_uuid,
       submit_date as submit_at,
       additional_comments
from {{ source('int_analytics', 'nps_scores') }}
where submit_date >= date_add('years',-2,date_trunc('year',getdate()))
