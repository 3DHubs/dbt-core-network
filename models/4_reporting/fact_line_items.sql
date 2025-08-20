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
 
  
select     li.order_uuid,
           -- Document fields
           li.document_uuid,
           li.document_type,
           li.document_revision,
           li.is_order_quote,
           li.is_active_po,
           -- Supply Line Items Base Fields
           li.created                                                                 as created_date,
           li.li_updated_at,
           li.id                                                                      as line_item_id,
           case when li.type = 'part' then
           row_number() over (partition by li.quote_uuid, li.type order by li.title,li.id asc)
           else null end                                                              as line_item_number,
           li.uuid                                                                    as line_item_uuid,
           li.quote_uuid,
           li.upload_id,
           li.correlation_uuid,
           li.technology_id,
           li.technology_name,
           li.material_id,
           li.material_subset_id,
           li.process_id,
           li.process_name,
           li.material_color_id,
           li.branded_material_id,
           li.shipping_option_id,
           li.type                                                                    as line_item_type,
           li.custom_material_subset_name                                             as custom_material_subset_name,
           (nullif(li.custom_material_subset_name, '') <> null)                       as has_custom_material_subset, --todo-migration-test = from is
           li.custom_finish_name                                                      as custom_surface_finish_name,
           (nullif(li.custom_finish_name, '') <> null)                                as has_custom_finish, --todo-migration-test = from is
           li.description                                                             as line_item_description, -- comment from customer 
           (nullif(li.description, '') <> null)                                       as has_customer_note, --todo-migration-test = from is
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
           li.material_name,
           li.is_expedited,
           li.is_vqced,
           li.commodity_code,
           -- Tolerances
           t.name                                                                       as tiered_tolerance,
           li.general_tolerance_class                                                   as general_tolerance,
           li.custom_tolerance,
           li.custom_tolerance_unit,

           -- Technology
           

           -- Materials, Processes & Finishes
           li.material_type_name,
           li.material_subset_name,
           li.material_density_g_cm3,
           li.material_color_name,
           li.branded_material_name,
           li.surface_finish_name,
           li.cosmetic_type,

           -- Dispute Fields
           d.dispute_created_at,
           d.dispute_created_at <> null                                                 as is_dispute, --todo-migration-test = from is
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
           c.created_at <> null as is_complaint, --todo-migration-test = from is
           c.is_valid as complaint_is_valid,
           c.is_conformity_issue as complaint_is_conformity_issue,
           c.outcome_customer as complaint_outcome_customer,
           c.outcome_supplier as complaint_outcome_supplier,
           c.resolution_at as complaint_resolution_at,
           c.created_by as complaint_created_by,
           c.reviewed_by as complaint_reviewed_by,
           c.comment as complaint_comment,
           c.claim_type as complaint_type,
           c.liability as complaint_liability,
           c.corrective_action_plan_needed,
           c.qc_comment,

           -- Part Dimensional Fields
           li.part_depth_cm,
           li.part_width_cm,
           li.part_height_cm,
           greatest(li.part_depth_cm, li.part_width_cm, li.part_height_cm) as part_longest_dimension,
           li.part_bounding_box_volume_cm3,
           li.part_smallest_bounding_box_volume_cm3,
           round(coalesce(li.upload_part_volume_cm3, (li.weight_in_grams/li.material_density_g_cm3)), 6) as part_volume_cm3,           
           round(coalesce(li.material_density_g_cm3 * li.upload_part_volume_cm3, li.weight_in_grams), 6) as part_weight_g,           
           round(li.part_bounding_box_volume_cm3 * li.quantity, 6)                   as line_item_total_bounding_box_volume_cm3,
           round(li.part_smallest_bounding_box_volume_cm3 * li.quantity, 6)          as line_item_total_smallest_bounding_box_volume_cm3,           
           round(part_volume_cm3 * li.quantity, 6)                                    as line_item_total_volume_cm3,
           round(part_weight_g * li.quantity, 6)                                      as line_item_weight_g,

           -- Amount Fields
           li.auto_price_amount,
           case
           --          If price amount is given always use this as it is the manually set amount
           when li.price_amount <> null then 
                li.price_amount

           --          Some non part line items have no unit price, thus we use auto_price amount (e.g. such as surcharge)
            when li.type != 'part' and li.auto_price_amount <> null then
                    coalesce(li.auto_price_amount, 0)

           --          When unit price amount is given a simple multiplication with the quantity (if 0 then 1) will do (both parts and non parts), if the
           --          order is of technology injection molding then we also add in tooling
           when li.unit_price_amount <> null then
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

           end  / 100.00                                                               as line_item_price_amount, --todo-migration-test = from is
           line_item_price_amount / rates.rate                                         as line_item_price_amount_usd,
           
           docs.currency_code                                                          as line_item_price_amount_source_currency,
           -- These amount fields are only manually inserted, nowadays only unit_price_amount is populated and the price_amount is calculated from the quantity
           coalesce(li.unit_price_amount, li.price_amount) <> null                 as line_item_price_amount_manually_edited, --todo-migration-test = from is
           (line_item_price_amount_usd -  case when li.unit_price_amount <> null then  (coalesce(li.tooling_price_amount, li.auto_tooling_price_amount, 0) / 100.00 / rates.rate) else 0 end)  * lic.estimated_l1_customs_rate  as line_item_estimated_l1_customs_amount_usd_no_winning_bid, --todo-migration-test = from is
           pv.quoting_package_version

    from {{ ref('prep_line_items') }} as li
             left join {{ ref('prep_supply_documents') }} as docs on docs.uuid = li.quote_uuid             

             -- Disputes
             left join {{ ref('agg_line_items_disputes') }} as d on d.line_item_uuid = li.uuid                             

             -- Complaints 
             left join {{ ref ('complaints')}} c on c.line_item_uuid = li.uuid

            -- Joins for exchange rates
             left join {{ ref('stg_orders_dealstage') }} as order_deals on docs.order_uuid = order_deals.order_uuid
             left join {{ ref('exchange_rate_daily') }} as rates
                             on rates.currency_code_to = docs.currency_code 
                             -- From '2022-04-01' we started using the more appropriate closing date as exchange rate date for closing values instead of quote finalized_at, this has been changed but not retroactively.
                             and date_trunc('day', coalesce(case when order_deals.closed_at >= '2022-04-01' then order_deals.closed_at else null end, docs.finalized_at, docs.created)) = date_trunc('day', rates.date) --todo-migration-test

             -- Other Joins
             left join {{ ref('tolerances') }} t on t.id = li.tolerance_id  
             left join {{ ref('stg_line_items_customs')}} lic on lic.uuid = li.uuid   
             left join {{ ref('package_version') }} pv on pv.correlation_uuid = li.correlation_uuid
