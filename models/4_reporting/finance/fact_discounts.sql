{{
    config(
        post_hook = "analyze {{ this }}",
        tags=["multirefresh"]
    )
}}

-- Created by: Jurien
-- Edited by: Diego
-- Last edit: Jan 2022

-- Description: This table contains visible discounts data, 
-- these exists as a single line item in the quote.

with supply_orders as (
    -- Reduce the number of orders to improve query time
    select distinct so.uuid, quote_uuid
    from {{ ref('prep_supply_orders') }} as so
    where exists(
                  select *
                  from {{ ref('prep_supply_documents') }} as soq
                  where status != 'cart'
                    and so.uuid = soq.order_uuid
              )
)
select so.uuid                                                  as order_uuid,

        -- Explanation: discounts are created by admins (e.g. 10% discount) but applicable
        -- through discount codes e.g. (CODE: YOURFIRSTDISCOUNT). 

        -- Discount Attributes
        ld.discount_id,
        ld.discount_title,
        true as has_discount,
        ld.discount_factor,
        -- Discount Codes Attributes
        ld.discount_code_id,
        ld.discount_code_id <> null as has_discount_code, --todo-migration-test <> from is not
        ld.discount_code,
        ld.discount_code_description,
        ld.discount_code_author_name as discount_code_created_by

-- Includes all line_items of type discount
from {{ ref('sources_network', 'gold_discount__line_items') }} as ld
         -- Prep line items filters on main quote and purchase orders
         inner join {{ ref('prep_line_items') }} as l on ld.uuid = l.uuid
         -- Filters out carts
         inner join supply_orders so on so.quote_uuid = l.quote_uuid