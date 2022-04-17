select *
from {{ source('int_service_supply', 'dispute_issues') }}