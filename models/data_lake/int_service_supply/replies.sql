select created,
       updated,
       deleted,
       uuid,
       question_uuid,
       value,
       {{ varchar_to_boolean('is_correct') }},
       reply_option_id,
       unit
from int_service_supply.replies