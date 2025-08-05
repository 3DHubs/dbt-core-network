select created_at,
       order_uuid,
       user_type,
       question_1,
       answer_1
from {{ ref('fact_order_reviews') }}
where created_at >= dateadd('year',-2,date_trunc('year',current_date())) --todo-migration-test
 --todo-migration-missing can't test because of snowflake access
