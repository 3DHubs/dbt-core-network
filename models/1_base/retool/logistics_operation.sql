
select 
    id                                                                                                       as logistics_operation_uuid,
    tracking_number                                                                                          as tracking_number,
    ordnum                                                                                                   as document_number,
    piece_count                                                                                              as package_piece_count,
    expected_shipping_costs,
    expected_shipping_costs_currency,
    service_level,
    package_handler,
    destination_code,
    total_weight                                                                                             as package_total_weight,

    -- Timestamp Fields
    to_timestamp(replace(replace(started_at, 'T', ' '), 'Z', ''), 'YYYY-MM-DD HH:MI:SS.MS')::timestamp       as logistics_operation_start_time,
    created_at                                                                                               as label_created_at,
    updated_at                                                                                               as logistics_operation_updated_at,
    extract(epoch from (label_created_at - logistics_operation_start_time))                                  as time_spent_in_seconds,
    
    -- Boolean Fields   
    {{ varchar_to_boolean('is_conforming')}}         

from {{ source('int_retool', 'dhl_api_documents') }} as logistics_operation


