{{
    config(
        materialized='incremental',
        pre_hook=["
            INSERT INTO {{ source('int_analytics', 'full_order_history_events') }}
            SELECT id,
                created,
                order_uuid,
                quote_uuid,
                line_item_uuid,
                user_id,
                anonymous_id,
                description,
                null as extra
            FROM {{ ref('sources_network', 'gold_order_history_events') }} as ns_ohe
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
            FROM {{ source('int_analytics', 'full_order_history_events') }} as dl_fohe
            where ns_ohe.id = dl_fohe.id)
                "],
        tags=["multirefresh"]
    )
}}

--todo-migration-research: full_order_history events needs to be placed in a proper schema
-- I removed the "extra" column that was semi-structured and was showing some issues
-- Verify if it is needed or how to process it properly

select id,
       created,
       order_uuid,
       quote_uuid,
       line_item_uuid,
       user_id,
       anonymous_id,
       description

from {{ source('int_analytics', 'full_order_history_events') }}

{% if is_incremental() %}

  where created > (select max(created) from {{ this }} )

{% endif %}
