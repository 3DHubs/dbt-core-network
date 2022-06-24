{{
    config(
        materialized='incremental',
        pre_hook=["
            INSERT INTO {{ source('data_lake', 'full_order_history_events') }}
            SELECT id,
                created,
                order_uuid,
                quote_uuid,
                line_item_uuid,
                user_id,
                anonymous_id,
                description
            FROM {{ source('int_service_supply', 'order_history_events') }} as iss_ohe
            WHERE NOT EXISTS (
                SELECT
                    id,
                    created,
                    order_uuid,
                    quote_uuid,
                    line_item_uuid,
                    user_id,
                    anonymous_id,
                    description
            FROM {{ source('data_lake', 'full_order_history_events') }} as dl_fohe
            where iss_ohe.id = dl_fohe.id)
                "],
        tags=["multirefresh"]
    )
}}

select id,
       created,
       order_uuid,
       quote_uuid,
       line_item_uuid,
       user_id,
       anonymous_id,
       description

from {{ source('data_lake', 'full_order_history_events') }}

{% if is_incremental() %}

  where id > (select max(id) from {{ this }})

{% endif %}