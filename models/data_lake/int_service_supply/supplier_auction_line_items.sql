{{
    config(
        materialized='incremental'
    )
}}

select *
from int_service_supply.supplier_auction_line_items

{% if is_incremental() %}

  where id > (select max(id) from {{ this }})

{% endif %}