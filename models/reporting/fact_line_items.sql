
{{
    config(
        pre_hook = "lock table {{ source('int_service_supply', 'line_items') }}",
        post_hook = "analyze {{ this }}"
    )
}}
 
with auction_tech as (
    select order_uuid, technology_id as auction_quotes_technology_id
        from (select auction_uuid,
                     winning_bid_uuid,
                     a_quotes.technology_id,
                     a_quotes.order_uuid,
                     row_number() over (partition by a_quotes.order_uuid order by auctions.status, auctions.auction_created_at desc) as seq
            from {{ ref('auctions_rda') }} as auctions
                    inner join {{ ref('cnc_order_quotes') }} as a_quotes on a_quotes.uuid = auctions.auction_uuid
            where auctions.is_latest_order_auction) aq
        where seq = 1
    ),
    line_items_tech as (
        select quote_uuid, line_item_technology_id, line_item_process_id
        from (select quote_uuid,
                    technology_id as                                                                      line_item_technology_id,
                    process_id    as                                                                      line_item_process_id,
                    quantity,
                    price_amount,
                    row_number() over (partition by quote_uuid order by quantity desc, price_amount desc) seq
            from {{ ref('line_items') }}
            where type = 'part') sqli
        where seq = 1
    ),
    orders_tech as (
        select base.uuid                                         as order_uuid,
           case
               when base.legacy_order_id is not null then 2 -- Legacy Orders can only be 3DP which is ID->2
               else coalesce(at.auction_quotes_technology_id,
                             li.line_item_technology_id) end as derived_order_technology_id
        from {{ ref('cnc_orders') }} base
                left outer join auction_tech at on base.uuid = at.order_uuid
                left join line_items_tech as li on li.quote_uuid = base.quote_uuid
    ),
    supply_orders as (
        select distinct so.uuid, quote_uuid
        from {{ ref('cnc_orders') }} as so
        where exists(
            select *
            from {{ ref('cnc_order_quotes') }} as soq
            where status != 'cart' and so.uuid = soq.order_uuid
        )
    ),
    part_dimensional_attributes as (
        select sqli.id,
           round(nullif(json_extract_path_text(sqli.upload_properties, 'volume', 'value', true), '')::float / 1000,
                 1)                                                 as part_volume_cm3,
           round(nullif(json_extract_path_text(sqli.upload_properties, 'natural_bounding_box', 'value', 'depth', true),
                        '')::float / 10, 1)                         as part_depth_cm,
           round(nullif(json_extract_path_text(sqli.upload_properties, 'natural_bounding_box', 'value', 'width', true),
                        '')::float / 10, 1)                         as part_width_cm,
           round(nullif(json_extract_path_text(sqli.upload_properties, 'natural_bounding_box', 'value', 'height', true),
                        '')::float / 10, 1)                         as part_height_cm,
           round(part_depth_cm * part_width_cm * part_height_cm, 1) as part_bounding_box_volume_cm3
        from {{ ref('line_items')}} as sqli
        where true
        and type = 'part'
        and upload_properties is not null
    )
select orders.uuid                                                                  as order_uuid,
           sqli.created                                                                 as created_date,
           sqli.updated                                                                 as updated_date,
           sqli.id                                                                      as line_item_id,
           case when sqli.type = 'part' then
           row_number() over (partition by sqli.quote_uuid, sqli.type order by sqli.id asc)
           else null
           end                                                                          as line_item_number,
           sqli.uuid                                                                    as line_item_uuid,
           sqli.quote_uuid,
           sqli.upload_id,
           sqli.technology_id,
           sqli.material_id,
           sqli.material_subset_id,
           sqli.process_id,
           sqli.material_color_id,
           sqli.branded_material_id,
           sqli.shipping_option_id,
           -- Dimensions
           sqli.type                                                                    as line_item_type,
           dt.name                                                                      as technology_name,
           mat.name                                                                     as material_name,
           msub.name                                                                    as material_subset_name,
           msub.density                                                                 as material_density_g_cm3,
           mc.name                                                                      as material_color_name,
           sqli.custom_material_subset_name                                             as custom_material_subset_name,
           (nullif(sqli.custom_material_subset_name, '') is not null)                   as has_custom_material_subset,
           prc.name                                                                     as process_name,
           bmat.name                                                                    as branded_material_name,
           mf.name                                                                      as surface_finish_name,
           sqli.custom_finish_name                                                      as custom_surface_finish_name,
           (nullif(sqli.custom_finish_name, '') is not null)                            as has_custom_finish,
           sqli.description                                                             as line_item_description, -- comment from customer
           (nullif(sqli.description, '') is not null)                                   as has_customer_note,
           sqli.admin_description,
           sqli.title                                                                   as line_item_title,       -- by default set to model file name; custom line_items: set by admin
           sqli.exceeds_standard_tolerances                                             as has_exceeded_standard_tolerances,
           sqli.should_quote_manually                                                   as needs_manual_quoting,  -- true if customer put custom material_subset
           sqli.has_technical_drawings,
           sqli.lead_time_options,
           sqli.part_orientation_additional_notes,
           sqli.part_orientation_vector,
           sqli.upload_properties,
           sqli.requires_specific_gate_position                                         as has_specific_gate_position,
           sqli.requires_specific_parting_line                                          as has_specific_parting_line,
           sqli.unit,
           d.dispute_created_at,
           d.dispute_created_at is not null                                             as is_dispute,
           d.dispute_description,
           d.dispute_affected_parts_quantity,
           d.has_tolerance_issue,
           d.has_incomplete_order_issue,
           d.has_missing_threads_issue,
           d.has_other_issue,
           d.has_surface_finish_issue,
           d.has_incorrect_material_issue,
           d.has_parts_damaged_issue,
           -- Complaints data
           c.created_at as complaint_created_at,
           c.created_at is not null as is_complaint,
           c.is_valid as complaint_is_valid,
           c.is_conformity_issue as complaint_is_conformity_issue,
           c.outcome_customer as complaint_outcome_customer,
           c.outcome_supplier as complaint_outcome_supplier,
           c.resolution_at as complaint_resolution_at,
           u.first_name + ' ' + u.last_name as complaint_created_by,
           ur.first_name + ' ' + ur.last_name as complaint_reviewed_by,
           c.comment as complaint_comment,
           c.claim_type as complaint_type,
           c.liability as complaint_liability,
           --c.number_of_parts as complaint_affected_parts_quantity, perhaps onboarded later.
           -- Part Dimensional Fields
           pdf.part_depth_cm,
           pdf.part_width_cm,
           pdf.part_height_cm,
           pdf.part_bounding_box_volume_cm3,
           round(pdf.part_bounding_box_volume_cm3 * sqli.quantity, 1)                   as line_item_total_bounding_box_volume_cm3,
           pdf.part_volume_cm3,
           round(pdf.part_volume_cm3 * sqli.quantity)                                   as line_item_total_volume_cm3,
           round(coalesce(msub.density * pdf.part_volume_cm3, sqli.weight_in_grams), 1) as part_weight_g,
           round(coalesce(part_weight_g, sqli.weight_in_grams) * sqli.quantity, 1)      as line_item_weight_g,
           sqli.quantity,
           sqli.has_threads,
           sqli.infill,
           sqli.layer_height,
           sqli.is_cosmetic,
           mf.cosmetic_type,
           t.name                                                                       as tiered_tolerance,
           case
               when sqli.type = 'part' and sqli.technology_id = 3
                   then round((coalesce(price_amount, unit_price_amount * quantity::double precision,
                                        auto_price_amount::double precision *
                                        coalesce(sqli.price_multiplier, soq.price_multiplier)) +
                               coalesce(tooling_price_amount, auto_tooling_price_amount)) / 100.00,
                              2) -- tooling costs only apply to IM parts
               when sqli.type = 'part'
                   then round(coalesce(price_amount, unit_price_amount * quantity::double precision,
                                       auto_price_amount::double precision *
                                       coalesce(sqli.price_multiplier, soq.price_multiplier)) / 100.00,
                              2) -- price_amount is the manually overridden price field and auto_price_amount is automatically generated. Only multiply by quantity if price is per unit
               else round(coalesce(price_amount, unit_price_amount * quantity::double precision,
                                   auto_price_amount::double precision) / 100.00,
                          2) -- price multiplier is only applied for parts
               end                                                                      as line_item_price_amount,
           case
               when sqli.type = 'part' and sqli.technology_id = 3
                   then (coalesce(price_amount, unit_price_amount * quantity::double precision,
                                  auto_price_amount::double precision *
                                  coalesce(sqli.price_multiplier, soq.price_multiplier)) +
                         coalesce(tooling_price_amount, auto_tooling_price_amount)) / 100.00 / rates.rate
               when sqli.type = 'part' then coalesce(price_amount, unit_price_amount * quantity::double precision,
                                                     auto_price_amount::double precision *
                                                     coalesce(sqli.price_multiplier, soq.price_multiplier)) / 100.00 /
                                            rates.rate
               else coalesce(price_amount, unit_price_amount * quantity::double precision,
                             auto_price_amount::double precision) / 100.00 / rates.rate
               end                                                                      as line_item_price_amount_usd,
           soq.currency_code                                                            as line_item_price_amount_source_currency
    from supply_orders as orders
             inner join {{ ref('line_items') }} as sqli on sqli.quote_uuid = orders.quote_uuid
             inner join {{ ref('cnc_order_quotes') }} as soq on soq.uuid = orders.quote_uuid
             left outer join orders_tech sot on sot.order_uuid = orders.uuid
             left outer join part_dimensional_attributes pdf on pdf.id = sqli.id
             left outer join {{ ref('technologies') }} dt on dt.technology_id = sot.derived_order_technology_id
             left outer join {{ ref('agg_line_items_disputes') }} as d on d.line_item_uuid = sqli.uuid
             left outer join {{ ref('materials') }} as mat on mat.material_id = sqli.material_id
             left outer join {{ ref('processes') }} as prc on prc.process_id = sqli.process_id
             left outer join {{ ref('material_subsets') }} as msub
                             on msub.material_subset_id = sqli.material_subset_id
             left outer join {{ ref('branded_materials') }} as bmat
                             on bmat.branded_material_id = sqli.branded_material_id
             left outer join {{ ref('material_finishes') }} as mf on sqli.finish_slug = mf.slug
             left outer join {{ ref('material_colors') }} as mc on sqli.material_color_id = mc.material_color_id -- TODO: does not exist.
             left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                             on rates.currency_code_to = soq.currency_code and trunc(soq.created) = trunc(rates.date)
             left outer join {{ ref('tolerances') }} t on t.id = sqli.tolerance_id
             left outer join {{ ref ('complaints')}} c on c.line_item_uuid = sqli.uuid
             left join {{ ref('users') }} u on u.user_id = c.created_by_user_id
             left join {{ ref('users') }} ur on ur.user_id = c.reviewed_by_user_id
    where sqli.legacy_id is null