select
    id,
    created_at,
    order_uuid,
    user_type,
    review_question              as question_1,
    review_score                 as answer_1,
    additional_feedback_question as question_2,
    additional_feedback_answer   as answer_2,
    user_full_name               as reviewed_by
from {{ ref('network_services', 'gold_order_reviews') }}
