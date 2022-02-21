 
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

           -- Supply Line Items Base Fields
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
           sqli.type                                                                    as line_item_type,
           sqli.custom_material_subset_name                                             as custom_material_subset_name,
           (nullif(sqli.custom_material_subset_name, '') is not null)                   as has_custom_material_subset,
           sqli.custom_finish_name                                                      as custom_surface_finish_name,
           (nullif(sqli.custom_finish_name, '') is not null)                            as has_custom_finish,
           sqli.description                                                             as line_item_description, -- comment from customer 
           (nullif(sqli.description, '') is not null)                                   as has_customer_note,
           sqli.admin_description,
           sqli.title                                                                   as line_item_title,       -- by default set to model file name; custom line_items: set by admin
           sqli.has_technical_drawings,
           sqli.lead_time_options,
           sqli.part_orientation_additional_notes,
           sqli.part_orientation_vector,
           sqli.upload_properties,
           sqli.requires_specific_gate_position                                         as has_specific_gate_position,
           sqli.requires_specific_parting_line                                          as has_specific_parting_line,
           sqli.unit,
           sqli.quantity,
           sqli.has_threads,
           sqli.infill,
           sqli.layer_height,
           sqli.is_cosmetic,

           -- Tolerances
           t.name                                                                       as tiered_tolerance,
           sqli.general_tolerance_class                                                 as general_tolerance,
           sqli.custom_tolerance,
           sqli.custom_tolerance_unit,

           -- Technology (Defined from multiple sources)
           dt.name                                                                      as technology_name,

           -- Materials, Processes & Finishes
           mat.name                                                                     as material_name,
           msub.name                                                                    as material_subset_name,
           msub.density                                                                 as material_density_g_cm3,
           mc.name                                                                      as material_color_name,
           prc.name                                                                     as process_name,
           bmat.name                                                                    as branded_material_name,
           mf.name                                                                      as surface_finish_name,
           mf.cosmetic_type,

           -- Dispute Fields
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

           -- Amount Fields
           case
           --          If price amount is given always use this as it is the manually set amount
           when sqli.price_amount is not null then
                sqli.price_amount



           --          Some non part line items have no unit price, thus we use auto_price amount (e.g. such as surcharge)
            when sqli.type != 'part' and sqli.auto_price_amount is not null then
                    coalesce(sqli.auto_price_amount, 0)

           --          When unit price amount is given a simple multiplication with the quantity (if 0 then 1) will do (both parts and non parts), if the
           --          order is of technology injection molding then we also add in tooling
           when sqli.unit_price_amount is not null then
                coalesce(sqli.unit_price_amount::double precision * coalesce(nullif(sqli.quantity, 0), 1) +
                coalesce(sqli.tooling_price_amount, sqli.auto_tooling_price_amount, 0),0)

           --          For all other line items auto_price_amount is given but still requires to be rounded appropriately.
           --          Unit prices should have no more decimals than 2, therefore, the auto_price_amount for the total line item is
           --          divided by the quantity and then rounded through the banker rounding method before multiplier again with the
           --          Quantity this ensures that the unit price is within 2 decimals and that the total is equal to unit price * q
           else

                case when abs(cast((sqli.auto_price_amount * soq.price_multiplier)/coalesce(nullif(sqli.quantity, 0), 1) as int) -
                                (sqli.auto_price_amount * soq.price_multiplier)/coalesce(nullif(sqli.quantity, 0), 1)) = 0.5 then
                        round((sqli.auto_price_amount * soq.price_multiplier)/coalesce(nullif(sqli.quantity, 0), 1)/2,0)*2
                else round((sqli.auto_price_amount * soq.price_multiplier)/coalesce(nullif(sqli.quantity, 0),1),0)
                end * coalesce(nullif(sqli.quantity, 0), 1)

           end  / 100.00                                                               as line_item_price_amount,
           line_item_price_amount / rates.rate                                         as line_item_price_amount_usd,
           
           soq.currency_code                                                            as line_item_price_amount_source_currency,
           -- These amount fields are only manually inserted, nowadays only unit_price_amount is populated and the price_amount is calculated from the quantity
           coalesce(sqli.unit_price_amount, sqli.price_amount) is not null              as line_item_price_amount_manually_edited

    from supply_orders as orders
             inner join {{ ref('line_items') }} as sqli on sqli.quote_uuid = orders.quote_uuid
             inner join {{ ref('cnc_order_quotes') }} as soq on soq.uuid = orders.quote_uuid           

             -- Technology
             left outer join orders_tech sot on sot.order_uuid = orders.uuid             
             left outer join {{ ref('technologies') }} dt on dt.technology_id = sot.derived_order_technology_id             

             -- Disputes
             left outer join {{ ref('agg_line_items_disputes') }} as d on d.line_item_uuid = sqli.uuid                             

             -- Materials Processes and Finishes
             left outer join {{ ref('materials') }} as mat on mat.material_id = sqli.material_id
             left outer join {{ ref('processes') }} as prc on prc.process_id = sqli.process_id
             left outer join {{ ref('material_subsets') }} as msub
                             on msub.material_subset_id = sqli.material_subset_id
             left outer join {{ ref('branded_materials') }} as bmat
                             on bmat.branded_material_id = sqli.branded_material_id
             left outer join {{ ref('material_finishes') }} as mf on sqli.finish_slug = mf.slug
             left outer join {{ ref('material_colors') }} as mc on sqli.material_color_id = mc.material_color_id -- TODO: does not exist.

             -- Complaints 
             left outer join {{ ref ('complaints')}} c on c.line_item_uuid = sqli.uuid
             left join {{ ref('users') }} u on u.user_id = c.created_by_user_id
             left join {{ ref('users') }} ur on ur.user_id = c.reviewed_by_user_id

             -- Other Joins
             left outer join part_dimensional_attributes pdf on pdf.id = sqli.id
             left outer join {{ ref('tolerances') }} t on t.id = sqli.tolerance_id             
             left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                             on rates.currency_code_to = soq.currency_code and trunc(soq.created) = trunc(rates.date)  

    where sqli.legacy_id is null