-- Quick test to see if I can make changes from git to DBT
select *
from {{ source('int_service_supply', 'branded_materials') }}
