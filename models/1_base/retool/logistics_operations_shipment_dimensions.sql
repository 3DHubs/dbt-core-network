select
    'eu'                                                                                                     as robowarehouse_region,
    md5(concat('eu_', id))                                                                                   as logistics_operation_shipment_dimensions_uuid,
    id                                                                                                       as logistics_operation_shipment_dimensions_id,
    ordnum                                                                                                   as document_number,
    length_cm                                                                                                as package_length_cm,
    width_cm                                                                                                 as package_width_cm,
    height_cm                                                                                                as package_height_cm,
    weight_kg                                                                                                as package_weight_kg,
    length_cm / 2.54                                                                                         as package_length_in, -- Convert cm to inches
    width_cm / 2.54                                                                                          as package_width_in, -- Convert cm to inches
    height_cm / 2.54                                                                                         as package_height_in, -- Convert cm to inches
    weight_kg / 0.45359237                                                                                   as package_weight_lb, -- Convert kg to pounds
    piece_count                                                                                              as package_piece_count,
    dhl_api_id                                                                                               as logistics_operation_id

from
    {{ source('int_retool', 'dhl_api_shipment_dimensions') }} as eu_logistics_operation_shipping_dimensions

union all

select
    'us'                                                                                                     as robowarehouse_region,
    md5(concat('us_', id))                                                                                   as logistics_operation_shipment_dimensions_uuid,
    id                                                                                                       as logistics_operation_shipment_dimensions_id,
    ordnum                                                                                                   as document_number,
    length_in * 2.54                                                                                         as package_length_cm, -- Convert inches to cm
    width_in * 2.54                                                                                          as package_width_cm, -- Convert inches to cm
    height_in * 2.54                                                                                         as package_height_cm, -- Convert inches to cm
    weight_lb * 0.45359237                                                                                   as package_weight_kg, -- Convert pounds to kg
    length_in                                                                                                as package_length_in,
    width_in                                                                                                 as package_width_in,
    height_in                                                                                                as package_height_in,
    weight_lb                                                                                                as package_weight_lb,
    piece_count                                                                                              as package_piece_count,
    ups_api_id                                                                                               as logistics_operation_id

from
    {{ source('int_retool', 'ups_api_shipment_dimensions') }} as us_logistics_operation_shipping_dimensions
