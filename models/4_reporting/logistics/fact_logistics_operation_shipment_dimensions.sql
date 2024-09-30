Select

    robowarehouse_region,
    logistics_operation_shipment_dimensions_uuid,
    logistics_operation_shipment_dimensions_id,
    document_number,
    package_length_cm,
    package_length_in,
    package_width_cm,
    package_width_in,
    package_height_cm,
    package_height_in,
    package_weight_kg,
    package_weight_lb,
    package_piece_count,
    logistics_operation_id

from {{ ref('logistics_operations_shipment_dimensions') }} as logistics_operation