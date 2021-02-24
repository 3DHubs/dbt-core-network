select created,
       updated,
       deleted,
       uuid,
       question_uuid,
       value,
       decode(is_correct, 'false', False, 'true', True) as is_correct,
       reply_option_id,
       unit
from int_service_supply.replies