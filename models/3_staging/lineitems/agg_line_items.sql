----------------------------------------------------------------
-- LINE ITEMS FIELDS
----------------------------------------------------------------

-- AGG LINE ITEMS
-- Aggregates data from Fact Line Items in the reporting layer
-- as we have important fields defined there. This table aggregates data from both 
-- quotes of type quote or type purchase orders.
{{ config(
    tags=["multirefresh"]
) }}
with agg_line_items as (
    select fli.quote_uuid, -- To join to orders on quote/purchase order quote uuid
           fli.order_uuid, -- Due to the filter at the bottom there should be 1-1 relationship in this table of quotes and orders

           -- Counts
           count(*)                                                                        as number_of_line_items, -- Used to filter stg_fact_orders on non-empty orders
           count(distinct fli.material_id)                                                 as number_of_materials,
           count(distinct fli.process_id)                                                  as number_of_processes,
           count(case when fli.line_item_type = 'part' then line_item_id end)              as number_of_part_line_items,

           -- Totals
           sum(case when fli.line_item_type = 'part' then nvl(fli.quantity, 0) else 0 end) as total_quantity,
           sum(case
                   when fli.line_item_type = 'part' then nvl(fli.line_item_weight_g, 0)
                   else 0 end)                                                             as total_weight_grams,
           sum(case
                   when fli.line_item_type = 'part' then nvl(fli.line_item_total_bounding_box_volume_cm3, 0)
                   else 0 end)                                                             as total_bounding_box_volume_cm3,
           sum(case
                   when fli.line_item_type = 'part' then nvl(fli.line_item_total_volume_cm3, 0)
                   else 0 end)                                                             as total_volume_cm3,

           -- Financial (Original Currency)    
           sum(fli.line_item_price_amount)                                                 as li_subtotal_amount, -- For reference, this value shoulds match the values in orders for quotes and pos
           sum(case
                   when fli.line_item_type = 'part' then line_item_price_amount
                   else 0 end)                                                             as parts_amount, 
           sum(case
                   when fli.line_item_type = 'shipping' then line_item_price_amount
                   else 0 end)                                                             as shipping_amount,
           sum(case
                   when fli.line_item_type = 'discount' then line_item_price_amount
                   else 0 end)                                                             as discount_cost,                   
           sum(case
                   when fli.line_item_type in ('custom', 'surcharge', 'machining-certification')
                       then line_item_price_amount
                   else 0 end)                                                             as other_line_items_amount,
           -- Financial (USD - Conversion based on Creation Date)        
           sum(line_item_price_amount_usd)                                                 as li_subtotal_amount_usd, -- For reference, this value shoulds match the values in orders for quotes and pos
           sum(fli.line_item_estimated_l1_customs_amount_usd)                              as estimated_l1_customs_amount_usd,
           sum(fli.line_item_estimated_l2_customs_amount_usd)                              as estimated_l2_customs_amount_usd,
           sum(case
                   when fli.line_item_type = 'part' then line_item_price_amount_usd
                   else 0 end)                                                             as parts_amount_usd, 
           sum(case
                   when fli.line_item_type = 'shipping' then line_item_price_amount_usd
                   else 0 end)                                                             as shipping_amount_usd,
           sum(case
                   when fli.line_item_type = 'discount' then line_item_price_amount_usd
                   else 0 end)                                                             as discount_cost_usd,                   
           sum(case
                   when fli.line_item_type in ('custom', 'surcharge', 'machining-certification')
                       then line_item_price_amount_usd
                   else 0 end)                                                             as other_line_items_amount_usd,
           sum(case
                   when fli.shipping_option_id in
                        (select distinct id
                         from data_lake.supply_shipping_options
                         where is_expedited is true) then 1
                   else 0 end)                                                             as number_of_expedited_shipping_line_items, -- Used for definition

           -- Boolean Aggregates
           bool_or(coalesce(fli.has_customer_note, false))                                 as has_customer_note,
           bool_or(coalesce(fli.has_technical_drawings, false))                            as has_technical_drawings,
           bool_or(coalesce(fli.has_custom_material_subset, false))                        as has_custom_material_subset,
           bool_or(coalesce(fli.has_custom_finish, false))                                 as has_custom_finish,
           bool_or(coalesce(fli.is_cosmetic, false))                                       as has_cosmetic_surface_finish,
           bool_or(coalesce(fli.is_vqced, false))                                          as has_vqc_line_item,
           bool_or(coalesce(so.is_expedited, false))                                       as is_expedited_shipping,
           bool_or(case
                       when lower(line_item_title) like ('%svp required%') then true
                       else false end)                                                     as has_svp_line_item
    from {{ ref('fact_line_items') }} as fli
        left join {{ ref('shipping_options') }} as so on fli.shipping_option_id = so.id
    group by 1,2

-- SEQ LINE ITEMS
-- Defines data of the order based on the values of the first line item
-- It queries from data lake layer as it requires basic fields

),
     sequence_line_items as (
         select sqli.quote_uuid,
                sqli.line_item_technology_id,
                sqli.line_item_technology_name,
                sqli.line_item_process_id,
                spr.name as line_item_process_name
         from (select fli.quote_uuid,
                      fli.process_id    as                                                                   line_item_process_id,
                      fli.technology_id as                                                                   line_item_technology_id,
                      t.name            as                                                                   line_item_technology_name,
                      row_number()
                      over (partition by quote_uuid order by quantity desc, line_item_price_amount_usd desc) seq
               from {{ ref('fact_line_items') }} as fli
                        left join {{ ref ('technologies') }} as t
               on fli.technology_id = t.technology_id
               where line_item_type = 'part'
              ) as sqli
                  left join {{ ref('processes') }} as spr
         on sqli.line_item_process_id = spr.process_id
         where seq = 1
     ),

     lists as (
         select fli.quote_uuid,
                listagg(distinct fli.line_item_title, ', ') within group (order by fli.line_item_title) as parts_titles
         from {{ ref('fact_line_items') }} as fli
         where fli.line_item_type = 'part'
         group by 1
     )

-- FINAL QUERY
-- Combines Fields from the AGGREGATED and SEQUENCE Tables

select agg.*,
        seq.line_item_technology_id,
        seq.line_item_technology_name,
        seq.line_item_process_id,
        seq.line_item_process_name,
        lists.parts_titles
from agg_line_items as agg
        left join sequence_line_items as seq on agg.quote_uuid = seq.quote_uuid
        left join lists as lists on agg.quote_uuid = lists.quote_uuid