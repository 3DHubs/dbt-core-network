Select

    form_id,
    response_id,
    submitted_at,
    submitted_at_chi,
    submitted_at_ams,
    response_type,
    supplier_id,
    boxes_mapping.dimension_mapping                                    as branded_box_dimensions,
    branded_box_quantity

from {{ ref('dbt_src_external', 'gold_airbyte_typeform_responses') }} as branded_boxes
    left join {{ ref('seed_branded_boxes_mapping') }} boxes_mapping on branded_boxes.branded_box_dimensions = boxes_mapping.dimensions