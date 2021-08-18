----------------------------------------------------------------
-- LINE ITEMS FIELDS
----------------------------------------------------------------

-- AGG LINE ITEMS
-- Aggregates data from Fact Line Items in the reporting layer 
-- as we have important fields defined there.

with agg_line_items as (
    select quote_uuid,-- To join to orders on order/active quote

           -- Counts
           count(distinct material_id)                                                                           as number_of_materials,
           count(distinct process_id)                                                                            as number_of_processes,
           count(case when line_item_type = 'part' then line_item_id end)                                        as number_of_part_line_items,

           -- Totals
           sum(case when line_item_type = 'part' then nvl(quantity, 0) else 0 end)                               as order_total_quantity, -- not leveraged
           sum(case when line_item_type = 'part' then nvl(line_item_weight_g,0) else 0 end)                      as order_total_weight_grams,
           sum(case when line_item_type = 'part' then nvl(line_item_total_bounding_box_volume_cm3,0) else 0 end) as order_total_bounding_box_volume_cm3,
           sum(case when line_item_type = 'part' then nvl(line_item_total_volume_cm3,0) else 0 end)              as order_total_volume_cm3,
           sum(case when line_item_type = 'shipping' then nvl(line_item_price_amount, 0) else 0 end)             as shipping_price_amount,
           sum(case when shipping_option_id in
                         (select distinct id
                          from data_lake.supply_shipping_options
                          where is_expedited is true) then 1 else 0 end)                                         as number_of_expedited_shipping_line_items, -- Used for definition

           -- Boolean Aggregates
           bool_or(coalesce(has_customer_note,false))                                                            as has_customer_note,
           bool_or(coalesce(has_exceeded_standard_tolerances,false))                                             as has_exceeded_standard_tolerances,
           bool_or(coalesce(has_technical_drawings,false))                                                       as has_technical_drawings,
           bool_or(coalesce(has_custom_material_subset,false))                                                   as has_custom_material_subset,
           bool_or(coalesce(has_custom_finish,false))                                                            as has_custom_finish,
           bool_or(coalesce(is_cosmetic,false))                                                                  as has_cosmetic_surface_finish,
           bool_or(coalesce(so.is_expedited,false))                                                              as is_expedited_shipping,
           bool_or(case when lower(line_item_title) like ('%svp required%') then true else false end)            as has_svp_line_item

           --todo: if agreed, remove fields below

--            expedited_shipping_name, -- This is actually leveraged in fact deals but not in Looker, I find this field confusing
--            sum(case
--                    when type = 'part' then nvl(auto_price_original_amount, 0) / 100
--                    else 0 end)                                                                 as part_auto_price_original_amount,
--            sum(case
--                    when type = 'part' and len(auto_price_amount) > 0
--                        then nvl(replace(trim(auto_price_amount), ' ', '0'), '0')::float / 100
--                    else 0 end)                                                                 as part_auto_price_amount,
--            sum(case when type = 'part' then nvl(price_amount, 0) / 100 else 0 end)             as part_price_amount,
--            sum(
--                    case when type = 'part' then nvl(tax_price_amount, 0) / 100 else 0 end)     as part_tax_amount,
--            sum(case
--                    when type = 'part'
--                        then nvl(auto_tooling_price_original_amount, 0) / 100
--                    else 0 end)                                                                 as part_auto_tooling_price_original_amount,
--            sum(case
--                    when type = 'part' then nvl(auto_tooling_price_amount, 0) / 100
--                    else 0 end)                                                                 as part_auto_tooling_price_amount,
--            sum(
--                    case when type = 'part' then nvl(tooling_price_amount, 0) / 100 else 0 end) as part_tooling_price_amount,
--            sum(
--                    case
--                        when type = 'shipping'
--                            then
--                                nvl(coalesce(price_amount, auto_price_amount::double precision), 0) /
--                                100
--                        else 0 end)                                                             as shipping_price_amount,
--            sum(
--                    case when type = 'shipping' then nvl(tax_price_amount, 0) / 100 else 0 end) as shipping_tax_amount,
--            sum(case
--                    when type = 'shipping' then nvl(tax_price_exempt_amount, 0) / 100
--                    else 0 end)                                                                 as shipping_tax_excempt_amount,
--            sum(
--                    case when type = 'surcharge' then nvl(price_amount, 0) / 100 else 0 end)    as surcharge_price_amount,
--            sum(case
--                    when type = 'surcharge' then nvl(tax_price_amount, 0) / 100
--                    else 0 end)                                                                 as surcharge_tax_amount,
--            sum(case
--                    when type = 'surcharge' then nvl(tax_price_exempt_amount, 0) / 100
--                    else 0 end)                                                                 as surcharge_tax_excempt_amount,

    from dbt_prod_reporting.fact_line_items as li
    left join {{ ref('shipping_options') }} as so on li.shipping_option_id = so.id
    group by 1

-- SEQ LINE ITEMS
-- Defines data of the order based on the values of the first line item
-- It queries from data lake layer as it requires basic fields

), sequence_line_items as (
    select sqli.quote_uuid,
           sqli.line_item_technology_id,
           sqli.line_item_technology_name,
           sqli.line_item_process_id,
           spr.name as line_item_process_name
    from (select li.quote_uuid,
                 li.process_id as line_item_process_id,
                 li.technology_id as line_item_technology_id,
                 t.name as line_item_technology_name,
                 row_number()
                 over (partition by quote_uuid order by quantity desc, price_amount desc) seq
          from {{ ref('line_items') }} as li
          left join {{ ref ('technologies') }} as t on li.technology_id = t.technology_id
          where type = 'part') as sqli
    left join {{ ref('processes') }} as spr on sqli.line_item_process_id = spr.process_id
    where seq = 1
)

-- FINAL QUERY
-- Combines Fields from the AGGREGATED and SEQUENCE Tables

select agg.*,
       round((agg.shipping_price_amount / rates.rate) , 2)::decimal(15,2) as shipping_amount_usd,
       seq.line_item_technology_id,
       seq.line_item_technology_name,
       seq.line_item_process_id,
       seq.line_item_process_name
from agg_line_items as agg
left join {{ ref('cnc_order_quotes') }} as quotes on agg.quote_uuid = quotes.uuid
left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
             on quotes.currency_code = rates.currency_code_to 
             and trunc(coalesce(quotes.finalized_at, quotes.created)) = trunc(rates.date) 
left join sequence_line_items as seq using(quote_uuid)
