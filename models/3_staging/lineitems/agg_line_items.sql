-- --------------------------------------------------------------
-- LINE ITEMS FIELDS
-- --------------------------------------------------------------
-- AGG LINE ITEMS
-- Aggregates data from Fact Line Items in the reporting layer
-- as we have important fields defined there. This table aggregates data from both 
-- quotes of type quote or type purchase orders.
{{ config(tags=["multirefresh"]) }}

-- Split fact line items by type for better structure, this subquery contains only
-- parts.
with
part_line_items as (
    select
        *,
        row_number() over (partition by quote_uuid order by quantity desc, line_item_price_amount_usd desc) as seq
    from {{ ref("fact_line_items") }}
    where line_item_type = 'part'

    -- This subquery contains line items different than parts (e.g. custom, shipping,
    -- others)
),

other_line_items as (
    select
        quote_uuid,
        line_item_type,
        line_item_price_amount,
        line_item_price_amount_usd,
        shipping_option_id,
        is_expedited,
        line_item_title
    from {{ ref("fact_line_items") }} where line_item_type != 'part'

),

-- Aggregates of all line items
agg_line_items as (
    select
        quote_uuid,  -- To join to orders on quote/purchase order quote uuid
        -- Used to filter stg_fact_orders on non-empty orders
        count(*)                        as number_of_line_items,
        sum(line_item_price_amount)     as li_subtotal_amount,
        -- For reference, this value shoulds match the values in orders for quotes and pos
        sum(line_item_price_amount_usd) as li_subtotal_amount_usd
    from {{ ref("fact_line_items") }}
    group by 1

    -- Aggregates of part line items
),

agg_part_line_items as (
    select
        quote_uuid,

        -- Counts
        count(*)                                                                 as number_of_part_line_items,
        count(distinct material_id)                                              as number_of_materials,
        count(distinct process_id)                                               as number_of_processes,

        -- Max (fields used in rnd pipelines)
        max(part_depth_cm)                                                       as parts_max_depth_cm,
        max(part_width_cm)                                                       as parts_max_width_cm,
        max(part_height_cm)                                                      as parts_max_heigth_cm,

        -- Totals
        -- case when statement is to guarantee that sum of group by that contains null returns null
        -- only needed for fields dependent on upload properties that can fail more often

        sum(quantity)                                                            as total_quantity,
        case
            when count(*) != count(line_item_weight_g)
                then null
            else sum(line_item_weight_g)
        end                                                                      as total_weight_grams,
        case
            when count(*) != count(line_item_total_volume_cm3)
                then null
            else sum(line_item_total_volume_cm3)
        end                                                                      as total_volume_cm3,
        case
            when count(*) != count(line_item_total_bounding_box_volume_cm3)
                then null
            else sum(line_item_total_bounding_box_volume_cm3)
        end                                                                      as total_bounding_box_volume_cm3,
        case
            when count(*) != count(line_item_total_smallest_bounding_box_volume_cm3)
                then null else
                sum(line_item_total_smallest_bounding_box_volume_cm3)
        end                                                                      as total_smallest_bounding_box_volume_cm3,
        case 
            when count(case when line_item_price_amount_manually_edited = true then 1 end) = count(*) 
                then 'all'
            when count(case when line_item_price_amount_manually_edited = true then 1 end) > 0 
                then 'some'
            else 'none' 
        end                                                                      as price_amount_manually_edited_status,
        count(case when line_item_price_amount_manually_edited = true then 1 end) as price_amount_manually_edited_count,


        -- Financial (Original Currency)    
        sum(line_item_price_amount)                                              as parts_amount,

        -- Financial (USD - Conversion based on Creation Date)
        sum(line_item_price_amount_usd)                                          as parts_amount_usd,
        sum(line_item_estimated_l1_customs_amount_usd_no_winning_bid)            as estimated_l1_customs_amount_usd_no_winning_bid,

        -- Boolean Aggregates
        bool_or(coalesce(has_customer_note, false))                                               as has_customer_note,
        bool_or(coalesce(has_technical_drawings, false))                                          as has_technical_drawings,
        bool_or(coalesce(has_custom_material_subset, false))                                      as has_custom_material_subset,
        bool_or(coalesce(has_custom_finish, false))                                               as has_custom_finish,
        bool_or(coalesce(is_cosmetic, false))                                                     as has_cosmetic_surface_finish,
        bool_or(coalesce(is_vqced, false))                                                        as has_vqc_line_item,
        bool_or(coalesce(auto_price_amount is null, false))                                       as is_supply_or_smart_rfq,
        bool_or(coalesce(lower(line_item_title) like ('%svp required%'), false)) as has_svp_line_item

    from part_line_items

    group by 1

    -- Aggregates of other line items
),

agg_other_line_items as (
    select
        oli.quote_uuid,
        sum(
            case when oli.line_item_type = 'shipping' then oli.line_item_price_amount else 0 end
        )                        as shipping_amount,
        sum(
            case when oli.line_item_type = 'discount' then oli.line_item_price_amount else 0 end
        )                        as discount_cost,
        sum(
            case when oli.line_item_type in ('custom', 'surcharge', 'machining-certification') then oli.line_item_price_amount else 0 end
        )                        as other_line_items_amount,
        sum(
            case when oli.line_item_type = 'shipping' then oli.line_item_price_amount_usd else 0 end
        )                        as shipping_amount_usd,
        sum(
            case when oli.line_item_type = 'discount' then oli.line_item_price_amount_usd else 0 end
        )                        as discount_cost_usd,
        sum(
            case when oli.line_item_type in ('custom', 'surcharge', 'machining-certification') then oli.line_item_price_amount_usd else 0 end
        )                        as other_line_items_amount_usd,

        bool_or(oli.line_item_title in ('Certificate of Conformance (CoC)', 'Certificate of Conformance'))                     as has_coc_certification,
        bool_or(oli.is_expedited) as is_expedited_shipping -- Applicable only for type shipping, case when not valid in bool_or
    from other_line_items as oli
    group by 1
),

-- Defines data of the order based on the values of the first line item
agg_first_line_items as (
    select
        quote_uuid,
        technology_id   as line_item_technology_id,
        technology_name as line_item_technology_name,
        process_id      as line_item_process_id,
        process_name    as line_item_process_name
    from part_line_items
    where seq = 1
),

-- Such as median listagg cannot be combined with other aggregations
lists as (
    select
        quote_uuid,
        listagg(distinct line_item_title, ', ') within group (
            order by line_item_title
        ) as parts_titles
    from part_line_items
    group by 1
)

-- FINAL QUERY
select
-- All line item aggregates
    ali.quote_uuid,
    ali.number_of_line_items,
    ali.li_subtotal_amount,
    ali.li_subtotal_amount_usd,
    -- Only parts
    apli.number_of_part_line_items,
    apli.number_of_materials,
    apli.number_of_processes,
    apli.parts_max_depth_cm,
    apli.parts_max_width_cm,
    apli.parts_max_heigth_cm,
    apli.total_quantity,
    apli.total_weight_grams,
    apli.total_volume_cm3,
    apli.total_bounding_box_volume_cm3,
    apli.total_smallest_bounding_box_volume_cm3,
    apli.parts_amount,
    apli.parts_amount_usd,
    apli.estimated_l1_customs_amount_usd_no_winning_bid,
    apli.has_customer_note,
    apli.has_technical_drawings,
    apli.has_custom_material_subset,
    apli.has_custom_finish,
    apli.has_cosmetic_surface_finish,
    apli.has_vqc_line_item,
    apli.is_supply_or_smart_rfq,
    apli.has_svp_line_item,
    apli.price_amount_manually_edited_status,
    apli.price_amount_manually_edited_count,

    -- Other line items
    aoli.shipping_amount,
    aoli.discount_cost,
    aoli.other_line_items_amount,
    aoli.shipping_amount_usd,
    aoli.discount_cost_usd,
    aoli.other_line_items_amount_usd,
    aoli.is_expedited_shipping,
    aoli.has_coc_certification,

    -- Based on first line item
    afli.line_item_technology_id,
    afli.line_item_technology_name,
    afli.line_item_process_id,
    afli.line_item_process_name,

    -- Lists (cannot be combined with other aggregates)
    l.parts_titles

from agg_line_items as ali
    left join agg_part_line_items as apli on ali.quote_uuid = apli.quote_uuid
    left join agg_other_line_items as aoli on ali.quote_uuid = aoli.quote_uuid
    left join agg_first_line_items as afli on ali.quote_uuid = afli.quote_uuid
    left join lists as l on ali.quote_uuid = l.quote_uuid
