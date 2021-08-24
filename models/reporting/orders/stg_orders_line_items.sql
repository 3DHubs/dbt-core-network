----------------------------------------------------------------
-- LINE ITEMS FIELDS
----------------------------------------------------------------

-- AGG LINE ITEMS
-- Aggregates data from Fact Line Items in the reporting layer 
-- as we have important fields defined there.

with agg_line_items as (
    select li.quote_uuid,-- To join to orders on order/active quote

           -- Counts
           count(distinct li.material_id) as                                              number_of_materials,
           count(distinct li.process_id) as                                               number_of_processes,
           count(distinct li.id) as                                                       number_of_line_items, -- Used to filter empty carts
           count(case when li.type = 'part' then line_item_id end) as                     number_of_part_line_items,

           -- Totals
           sum(case when li.type = 'part' then nvl(li.quantity, 0) else 0 end) as         order_total_quantity, -- not leveraged
           sum(case
                   when li.type = 'part' then nvl(fli.line_item_weight_g, 0)
                   else 0 end) as                                                         order_total_weight_grams,
           sum(case
                   when li.type = 'part' then nvl(fli.line_item_total_bounding_box_volume_cm3, 0)
                   else 0 end) as                                                         order_total_bounding_box_volume_cm3,
           sum(case
                   when li.type = 'part' then nvl(fli.line_item_total_volume_cm3, 0)
                   else 0 end) as                                                         order_total_volume_cm3,
           sum(case when li.type = 'shipping' then nvl(li.price_amount, 0) else 0 end) as shipping_price_amount,
           sum(case
                   when li.shipping_option_id in
                        (select distinct id
                         from data_lake.supply_shipping_options
                         where is_expedited is true) then 1
                   else 0 end) as                                                         number_of_expedited_shipping_line_items, -- Used for definition

           -- Boolean Aggregates
           bool_or(coalesce(fli.has_customer_note, false)) as                             has_customer_note,
           bool_or(coalesce(fli.has_exceeded_standard_tolerances, false)) as              has_exceeded_standard_tolerances,
           bool_or(coalesce(fli.has_technical_drawings, false)) as                        has_technical_drawings,
           bool_or(coalesce(fli.has_custom_material_subset, false)) as                    has_custom_material_subset,
           bool_or(coalesce(fli.has_custom_finish, false)) as                             has_custom_finish,
           bool_or(coalesce(li.is_cosmetic, false)) as                                    has_cosmetic_surface_finish,
           bool_or(coalesce(so.is_expedited, false)) as                                   is_expedited_shipping,
           bool_or(case
                       when lower(line_item_title) like ('%svp required%') then true
                       else false end) as                                                 has_svp_line_item

    from {{ ref('line_items') }} as li
    left join {{ ref('fact_line_items') }} as fli on li.id = fli.line_item_id
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
