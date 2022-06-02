-- This model queries from int_service_supply.line_items and considerably filters the data to improve the performance of models downstream.
-- Furthermore this model is combined with a few selected fields from supply_documents (cnc_order_quotes) to facilitate identifying the 
-- characteristics of the document (quote or purchase orders) the line item belongs to. 


{% set boolean_fields = [
    "exceeds_standard_tolerances",
    "has_threads",
    "should_quote_manually",
    "has_technical_drawings",
    "requires_specific_gate_position",
    "requires_specific_parting_line",
    "requires_rapid_tooling",
    "is_visible_to_supplier",
    "is_cosmetic"
    ]
%}

select  

    -- Fields from Documents
    -- Useful to filter line items from different documents and their statuses
       docs.order_uuid,
       docs.uuid             as document_uuid,
       docs.type             as document_type,
       docs.revision         as document_revision,
       docs.is_order_quote,
       docs.is_active_po,
       docs.updated          as doc_updated_at,
       docs.order_updated_at as order_updated_at,
    -- Line Item Fields
       li.created,
       li.updated            as li_updated_at,
       li.deleted,
       li.id,
       li.uuid,
       li.quote_uuid,
       li.upload_id,
       li.type,
       li.quantity,
       li.auto_price_original_amount,
       li.price_amount,
       li.price_variations,
       li.unit,
       li.custom_material_subset_name,
       li.finish_slug,
       li.custom_finish_name,
       li.title,
       li.description,
       li.admin_description,
       li.auto_price_amount,
       li.unit_price_amount,
       li.material_subset_id,
       li.material_id,
       li.process_id,
       li.technology_id,
       li.parent_uuid,
       li.auto_tooling_price_amount,
       li.auto_tooling_price_original_amount,
       li.tooling_price_amount,
       li.infill,
       li.layer_height,
       li.material_color_id,
       li.lead_time_options,
       li.legacy_id,
       li.branded_material_id,
       li.part_orientation_additional_notes,
       li.part_orientation_vector,
       li.weight_in_grams,
       li.tax_code,
       li.tax_details,
       li.tax_price_amount,
       li.tax_price_exempt_amount,
       li.upload_properties,
       li.price_multiplier,
       li.commodity_code,
       li.shipping_option_id,
       li.target_margin,
       li.commodity_code_source,
       li.discount_id,
       li.discount_code_id,
       li.tolerance_id,
       li.custom_tolerance,
       li.custom_tolerance_unit,
       li.general_tolerance_class,
       li.estimated_price_amount,
       li.estimated_price_original_amount,
       li.thickness,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}

from {{ source('int_service_supply', 'line_items') }} as li
inner join {{ ref('prep_supply_documents') }} as docs on li.quote_uuid = docs.uuid
where true
    -- Filter: only interested until now on the main quote and purchase orders
    and (is_order_quote or docs.type = 'purchase_order')    
    -- Filter: only interested on quotes that are not in the cart status
    and docs.status <> 'cart'
    -- Filter: not interested on line items from legacy orders
    and li.legacy_id is null