{{
    config(
        materialized='incremental',
        unique_key='uuid',
        tags=["multirefresh"],
        post_hook=["delete from {{ this }}  where uuid not in (select uuid from {{ source('int_service_supply', 'line_items') }} )"]
    )
}}

{% set boolean_fields = [
    "exceeds_standard_tolerances",
    "has_threads",
    "has_fits",
    "has_internal_corners",
    "has_part_marking",
    "should_quote_manually",
    "has_technical_drawings",
    "is_visible_to_supplier",
    "is_cosmetic"
    ]
%}

select  

    -- Fields from Documents
    -- Useful to filter line items from different documents and their statuses
    -- Line Item Fields
       li.created,
       li.updated            as li_updated_at,
       li.deleted,
       li.id,
       li.uuid,
       li.quote_uuid,
       li.correlation_uuid,
       li.upload_id,
       li.type,
       li.quantity,
       li.auto_price_original_amount,
       li.price_amount,
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
       nullif(li.upload_properties, 'null') as upload_properties,
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
       li.estimated_first_leg_customs_amount_usd,
       li.estimated_second_leg_customs_amount_usd,
       li.estimated_price_amount,
       li.estimated_price_original_amount,
       li.thickness,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}

from {{ source('int_service_supply', 'line_items') }} as li
where true
    and li.legacy_id is null
    
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    and (li.updated >= (select max(li_updated_at) from {{ this }}) )

    {% endif %}
