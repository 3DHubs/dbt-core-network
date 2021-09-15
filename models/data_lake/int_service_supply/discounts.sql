select 
       created,
       updated,
       id,
       title,
       description,
       discount_factor,
       type,
       currency_code,
       discount_value,
       technology_id,
       {{ varchar_to_boolean('is_hidden') }} 
from {{ source('int_service_supply', 'discounts') }}