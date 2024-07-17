Select

    logistics_operation_uuid,
    logistics_operation_start_time,
    tracking_number,
    document_number,
    package_piece_count,
    expected_shipping_costs,
    expected_shipping_costs_currency,
    service_level,    
    time_spent_in_seconds,
    label_created_at,
    package_handler,
    destination_code,
    package_total_weight,
    logistics_operation_updated_at,

    -- Boolean Fields   
    is_conforming    
    
from {{ ref('logistics_operation') }} as logistics_operation