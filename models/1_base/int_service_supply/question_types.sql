select *
from {{ source('int_service_supply', 'question_types') }}