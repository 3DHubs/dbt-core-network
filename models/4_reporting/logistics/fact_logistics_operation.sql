Select

    robowarehouse_region,
    logistics_operation_uuid,
    logistics_operation_id,
    logistics_operation_start_time,
    tracking_number,
    document_number,
    package_piece_count,
    expected_shipping_costs_usd,
    service_level,    
    time_spent_in_seconds,
    label_created_at,
    package_handler,
    destination_code,
    destination_state_code,
    destination_zip_code,
    destination_company,
    package_total_weight_kg,
    package_total_weight_lb,
    logistics_operation_updated_at,

    -- Boolean Fields   
    is_conforming    
    
from {{ ref('logistics_operation') }} as logistics_operation