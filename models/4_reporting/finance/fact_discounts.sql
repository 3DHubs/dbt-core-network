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
        l.discount_id,
        d.title as discount_title,
        true as has_discount,
        d.discount_factor,

        -- Discount Codes Attributes
       discount_code_id,
       discount_code_id is not null as has_discount_code,
       dc.code as discount_code,
       dc.description as discount_code_description,
       u.first_name + ' ' + u.last_name as discount_code_created_by

from {{ ref('prep_line_items') }} l
         inner join {{ ref('prep_supply_documents') }} coq on coq.uuid = l.quote_uuid
         inner join supply_orders so on so.quote_uuid = l.quote_uuid
         inner join {{ ref('discounts') }} d on d.id = l.discount_id
         left join  {{ ref('discount_codes') }} dc on dc.id = l.discount_code_id
         left join  {{ ref('prep_users') }} u on u.user_id = dc.author_id
where l.type='discount'
