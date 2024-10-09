{{
    config(
        materialized='incremental',
        on_schema_change='sync_all_columns',
        unique_key='uuid',
        tags=["multirefresh"],
        post_hook=["delete from {{ this }}  where uuid not in (select uuid from {{ source('int_service_supply', 'line_items') }} )"]
    )
}}


select  
       created,
       updated as li_updated_at,
       id,
       uuid,
       quote_uuid,
       correlation_uuid,
       upload_id,
       type,
       quantity,
       auto_price_original_amount,
       price_amount,
       unit,
       custom_material_subset_name,
       finish_slug,
       custom_finish_name,
       title,
       description,
       admin_description,
       auto_price_amount,
       unit_price_amount,
       material_subset_id,
       material_id,
       process_id,
       technology_id,
       parent_uuid,
       auto_tooling_price_amount,
       tooling_price_amount,
       infill,
       layer_height,
       material_color_id,
       material_color_name,
       lead_time_options,
       legacy_id,
       branded_material_id,
       branded_material_name,
       part_orientation_additional_notes,
       part_orientation_vector,
       weight_in_grams,
       tax_price_amount,
       upload_properties,
       price_multiplier,
       commodity_code,
       shipping_option_id,
       discount_id,
       discount_code_id,
       tolerance_id,
       custom_tolerance,
       custom_tolerance_unit,
       general_tolerance_class,
       exceeds_standard_tolerances,
       has_threads,
       has_fits,
       has_internal_corners,
       has_part_marking,
       has_technical_drawings,
       is_cosmetic,
       process_name

from {{ ref('gold_line_items') }} as li
where true
    and legacy_id is null
    
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    and (updated >= (select max(li_updated_at) from {{ this }}) )

    {% endif %}
