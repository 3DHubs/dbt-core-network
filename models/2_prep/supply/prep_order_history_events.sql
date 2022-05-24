{{
    config(
        materialized='incremental'
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