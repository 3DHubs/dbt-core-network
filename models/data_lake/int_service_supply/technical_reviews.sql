select *
from {{ source('int_service_supply', 'technical_reviews') }}