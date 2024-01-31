-- Base model for replies table
-- Only leveraged columns downstream are included

select
question_uuid,
reply_option_id,
value,
unit,
is_correct
from {{ source('fed_fulfilment', 'replies') }}