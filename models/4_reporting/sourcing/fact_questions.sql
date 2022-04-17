select sq.uuid                                                                   as question_uuid,
       sq.order_uuid,
       sq.line_item_uuid,
       sq.status                                                                 as question_status,
       sq.submitted_at,
       sq.answered_at,
       round(extract(minutes from (sq.answered_at - sq.submitted_at)) / 1440, 1) as question_response_time,
       sq.title                                                                  as question_type,
       su_auth.first_name + ' ' + su_auth.last_name                              as author_name,
       su_answ.first_name + ' ' + su_answ.last_name                              as answered_name
from {{ ref('questions') }} sq
        left join {{ ref('users') }} su_auth on su_auth.user_id = sq.author_id
        left join {{ ref('users') }} su_answ on su_answ.user_id = sq.answered_by_id