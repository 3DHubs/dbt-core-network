select 
       created,
       updated,
       id,
       code,
       description,
       discount_id,
       technology_id,
       expires_at,
       only_first_order,
       deleted,
       author_id
       from {{ source('int_service_supply', 'discount_codes') }}