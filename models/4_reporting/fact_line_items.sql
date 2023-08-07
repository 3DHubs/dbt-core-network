 {{
    config(
        post_hook = "analyze {{ this }}",
        tags=["multirefresh"]
    )
}}

-- This model queries from the underlying line items model which is already filtered to include only line items
-- from non-empty orders, only from the main quote of the order, the first PO or the active PO. This model then
-- contains data about line items from different documents, the model fact_quote_line_items downstream is filtered
-- to include only line items from the main quote of the order.
 
 
with part_dimensional_attributes as (
        select li.id,
           round(nullif(json_extract_path_text(li.upload_properties, 'volume', 'value', true), '')::float / 1000,
                 6)                                                 as upload_part_volume_cm3, -- prefix to make origin explicit
           round(nullif(json_extract_path_text(li.upload_properties, 'natural_bounding_box', 'value', 'depth', true),
                        '')::float / 10, 6)                         as part_depth_cm,
           round(nullif(json_extract_path_text(li.upload_properties, 'natural_bounding_box', 'value', 'width', true),
                        '')::float / 10, 6)                         as part_width_cm,
           round(nullif(json_extract_path_text(li.upload_properties, 'natural_bounding_box', 'value', 'height', true),
                        '')::float / 10, 6)                         as part_height_cm,
           round(nullif(json_extract_path_text(li.upload_properties, 'smallest_bounding_box', 'value', 'depth', true),
                        '')::float / 10, 6)                         as smallest_bounding_box_depth_cm,
           round(nullif(json_extract_path_text(li.upload_properties, 'smallest_bounding_box', 'value', 'width', true),
                        '')::float / 10, 6)                         as smallest_bounding_box_width_cm,
           round(nullif(json_extract_path_text(li.upload_properties, 'smallest_bounding_box', 'value', 'height', true),
                        '')::float / 10, 6)                         as smallest_bounding_box_height_cm,                        
           round(part_depth_cm * part_width_cm * part_height_cm, 6) as part_bounding_box_volume_cm3,
           round(part_depth_cm * part_width_cm * part_height_cm, 6) as part_smallest_bounding_box_volume_cm3

        from {{ ref('prep_line_items')}} as li
        where true
        and type = 'part'
        and upload_properties is not null
        and is_order_quote
    ),
 vqc as (
    select li.uuid, 
        min(livqc.file_uuid) as livqc_file, 
        case when livqc_file is not null then true else false end as is_vqced
    from {{ ref('line_items') }} as li
    left join {{ source('int_service_supply', 'line_item_virtual_quality_control_part_photos') }} as livqc on li.uuid = livqc.line_item_uuid  
    group by li.uuid
)

    
select     li.order_uuid,
           -- Document fields
           li.document_uuid,
           li.document_type,
           li.document_revision,
           li.estimated_first_leg_customs_amount_usd / 100.00                         as line_item_estimated_l1_customs_amount_usd,
           li.estimated_second_leg_customs_amount_usd / 100.00                        as line_item_estimated_l2_customs_amount_usd,
           li.is_order_quote,
           li.is_active_po,
           -- Supply Line Items Base Fields
           li.created                                                                 as created_date,
           li.li_updated_at,
           li.id                                                                      as line_item_id,
           case when li.type = 'part' then
           row_number() over (partition by li.quote_uuid, li.type order by li.id asc)
           else null end                                                              as line_item_number,
           li.uuid                                                                    as line_item_uuid,
           li.quote_uuid,
           li.upload_id,
           li.correlation_uuid,
           li.technology_id,
           li.material_id,
           li.material_subset_id,
           li.process_id,
           li.material_color_id,
           li.branded_material_id,
           li.shipping_option_id,
           li.type                                                                    as line_item_type,
           li.custom_material_subset_name                                             as custom_material_subset_name,
           (nullif(li.custom_material_subset_name, '') is not null)                   as has_custom_material_subset,
           li.custom_finish_name                                                      as custom_surface_finish_name,
           (nullif(li.custom_finish_name, '') is not null)                            as has_custom_finish,
           li.description                                                             as line_item_description, -- comment from customer 
           (nullif(li.description, '') is not null)                                   as has_customer_note,
           li.admin_description,
           li.title                                                                   as line_item_title,       -- by default set to model file name; custom line_items: set by admin
           li.has_technical_drawings,
           li.lead_time_options,
           li.part_orientation_additional_notes,
           li.part_orientation_vector,
           li.upload_properties,
           li.unit,
           li.quantity,
           li.has_threads,
           li.has_fits,
           li.has_internal_corners,
           li.has_part_marking,
           li.infill,
           li.layer_height,
           li.is_cosmetic,
           vqc.is_vqced,

           -- Tolerances
           t.name                                                                       as tiered_tolerance,
           li.general_tolerance_class                                                   as general_tolerance,
           li.custom_tolerance,
           li.custom_tolerance_unit,

           -- Technology
           dt.name                                                                      as technology_name,

           -- Materials, Processes & Finishes
           mat.name                                                                     as material_name,
           mt.name                                                                      as material_type_name,
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
           c.corrective_action_plan_needed,
           c.qc_comment,

           -- Part Dimensional Fields
           pdf.part_depth_cm,
           pdf.part_width_cm,
           pdf.part_height_cm,
           case when pdf.part_depth_cm >= part_width_cm and pdf.part_depth_cm >= pdf.part_height_cm then pdf.part_depth_cm
                when pdf.part_width_cm >= pdf.part_depth_cm and pdf.part_width_cm >= pdf.part_height_cm then pdf.part_width_cm
                else pdf.part_height_cm end as part_longest_dimension,
           pdf.part_bounding_box_volume_cm3,
           pdf.part_smallest_bounding_box_volume_cm3,
           round(coalesce(pdf.upload_part_volume_cm3, (li.weight_in_grams/msub.density)), 6) as part_volume_cm3,           
           round(coalesce(msub.density * pdf.upload_part_volume_cm3, li.weight_in_grams), 6) as part_weight_g,           
           round(pdf.part_bounding_box_volume_cm3 * li.quantity, 6)                   as line_item_total_bounding_box_volume_cm3,
           round(pdf.part_smallest_bounding_box_volume_cm3 * li.quantity, 6)          as line_item_total_smallest_bounding_box_volume_cm3,           
           round(part_volume_cm3 * li.quantity, 6)                                    as line_item_total_volume_cm3,
           round(part_weight_g * li.quantity, 6)                                      as line_item_weight_g,

           -- Amount Fields
           li.auto_price_amount,
           case
           --          If price amount is given always use this as it is the manually set amount
           when li.price_amount is not null then
                li.price_amount

           --          Some non part line items have no unit price, thus we use auto_price amount (e.g. such as surcharge)
            when li.type != 'part' and li.auto_price_amount is not null then
                    coalesce(li.auto_price_amount, 0)

           --          When unit price amount is given a simple multiplication with the quantity (if 0 then 1) will do (both parts and non parts), if the
           --          order is of technology injection molding then we also add in tooling
           when li.unit_price_amount is not null then
                coalesce(li.unit_price_amount::double precision * coalesce(nullif(li.quantity, 0), 1) +
                coalesce(li.tooling_price_amount, li.auto_tooling_price_amount, 0),0)

           --          For all other line items auto_price_amount is given but still requires to be rounded appropriately.
           --          Unit prices should have no more decimals than 2, therefore, the auto_price_amount for the total line item is
           --          divided by the quantity and then rounded through the banker rounding method before multiplier again with the
           --          Quantity this ensures that the unit price is within 2 decimals and that the total is equal to unit price * q
           else

                case when abs(cast((li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0), 1) as int) -
                                (li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0), 1)) = 0.5 then
                        round((li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0), 1)/2,0)*2
                else round((li.auto_price_amount * docs.price_multiplier)/coalesce(nullif(li.quantity, 0),1),0)
                end * coalesce(nullif(li.quantity, 0), 1)

           end  / 100.00                                                               as line_item_price_amount,
           line_item_price_amount / rates.rate                                         as line_item_price_amount_usd,
           
           docs.currency_code                                                          as line_item_price_amount_source_currency,
           -- These amount fields are only manually inserted, nowadays only unit_price_amount is populated and the price_amount is calculated from the quantity
           coalesce(li.unit_price_amount, li.price_amount) is not null                 as line_item_price_amount_manually_edited

    from {{ ref('prep_line_items') }} as li
             left join {{ ref('prep_supply_documents') }} as docs on docs.uuid = li.quote_uuid

             -- Technology
             left join {{ ref('technologies') }} dt on dt.technology_id = li.technology_id             

             -- Disputes
             left join {{ ref('agg_line_items_disputes') }} as d on d.line_item_uuid = li.uuid                             

             -- Materials Processes and Finishes
             left join {{ ref('materials') }} as mat on mat.material_id = li.material_id
             left join {{ source('int_service_supply', 'material_types') }}  as mt on mt.material_type_id = mat.material_type_id
             left join {{ ref('processes') }} as prc on prc.process_id = li.process_id
             left join {{ ref('prep_material_subsets') }} as msub on msub.material_subset_id = li.material_subset_id
             left join {{ source('int_service_supply', 'branded_materials') }} as bmat on bmat.branded_material_id = li.branded_material_id
             left join {{ ref('material_finishes') }} as mf on li.finish_slug = mf.slug
             left join {{ source('int_service_supply', 'material_colors') }} as mc on li.material_color_id = mc.material_color_id -- TODO: does not exist.

             -- Complaints 
             left join {{ ref ('complaints')}} c on c.line_item_uuid = li.uuid
             left join {{ ref('prep_users') }} u on u.user_id = c.created_by_user_id
             left join {{ ref('prep_users') }} ur on ur.user_id = c.reviewed_by_user_id

            -- Joins for exchange rates
             left join {{ ref('stg_orders_dealstage') }} as order_deals on docs.order_uuid = order_deals.order_uuid
             left join {{ ref('exchange_rate_daily') }} as rates
                             on rates.currency_code_to = docs.currency_code 
                             -- From '2022-04-01' we started using the more appropriate closing date as exchange rate date for closing values instead of quote finalized_at, this has been changed but not retroactively.
                             and trunc(coalesce(case when order_deals.closed_at >= '2022-04-01' then order_deals.closed_at else null end, docs.finalized_at, docs.created)) = trunc(rates.date)

             -- Other Joins
             left join part_dimensional_attributes pdf on pdf.id = li.id
             left join vqc on li.uuid = vqc.uuid
             left join {{ ref('tolerances') }} t on t.id = li.tolerance_id      
