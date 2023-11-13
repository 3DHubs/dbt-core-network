select created_at,
       order_uuid,
       user_type,
       question_1,
       answer_1
from {{ ref('fact_order_reviews') }}
where created_at >= date_add('years',-2,date_trunc('year',getdate()))
