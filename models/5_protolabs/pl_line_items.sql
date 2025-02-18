{{ config(tags=["multirefresh"]) }}

select
    fql.line_item_uuid,
    fql.order_uuid,
    fql.line_item_type,

    fql.line_item_price_amount_usd,
    fql.line_item_cost_usd,
    fql.line_item_price_amount_source_currency,
    fql.created_date,
    fql.line_item_number,
    fql.upload_id,
    fql.technology_id,
    fql.material_id,
    fql.material_subset_id,
    fql.process_id,
    fql.material_color_id,
    fql.branded_material_id,
    fql.custom_material_subset_name,
    fql.has_custom_material_subset,
    fql.custom_surface_finish_name,
    fql.has_custom_finish,
    fql.lead_time_options,
    fql.quantity,
    fql.has_threads,
    fql.general_tolerance,
    fql.custom_tolerance,
    fql.custom_tolerance_unit,
    fql.technology_name,
    fql.material_name,
    fql.material_type_name,
    fql.material_subset_name,
    fql.material_color_name,
    fql.process_name,
    fql.branded_material_name,
    fql.surface_finish_name,
    fql.cosmetic_type,
    fql.has_surface_finish_issue,
    fql.part_depth_cm,
    fql.part_width_cm,
    fql.part_height_cm,
    fql.part_bounding_box_volume_cm3,
    fql.line_item_total_bounding_box_volume_cm3,
    fql.part_volume_cm3,
    fql.line_item_total_volume_cm3,
    fql.has_fits, 
    fql.has_internal_corners, 
    fql.has_part_marking, 
    fql.tiered_tolerance,

    fql.is_complaint,
    fql.complaint_is_valid,
    fql.complaint_created_at,
    fql.complaint_resolution_at,
    fql.complaint_is_conformity_issue,
    fql.dispute_created_at,
    fql.complaint_liability,
    fql.complaint_type,

    fql.complaint_outcome_customer,
    fql.complaint_outcome_supplier,
    fql.complaint_created_by,
    fql.complaint_reviewed_by,
    fql.complaint_comment,
    fql.corrective_action_plan_needed,
    fql.qc_comment,

    ----reorder fields
    coalesce(reorders.is_line_reorder,false)                                                        as is_line_reorder

from {{ ref("fact_quote_line_items") }} fql
    left join {{ ref('pl_prep_line_items_reorders') }} as reorders on fql.line_item_uuid = reorders.line_item_uuid

where fql.created_date >= '2019-01-01'

