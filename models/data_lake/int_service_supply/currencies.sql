-- The only reason why it's added as a model here is that it available as a model in dbt_prod_data_lake. Currently no downstream dependencies.
select *
from {{ source('int_service_supply', 'currencies') }}