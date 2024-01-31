select
uuid,
order_uuid,
line_item_uuid,
status,
submitted_at,
author_id,
answered_at,
answered_by_id,
question_text,
answer_text,
answer_attachment_uuid
from 
{{ source('fed_fulfilment', 'open_questions') }}