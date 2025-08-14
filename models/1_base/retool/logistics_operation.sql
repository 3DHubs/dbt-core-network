select
    'eu'                                                                                                     as robowarehouse_region,
    md5(concat('eu_', id))                                                                                   as logistics_operation_uuid,
    id                                                                                                       as logistics_operation_id,
    tracking_number,                                                                                          
    ordnum                                                                                                   as document_number,
    cast(piece_count as integer)                                                                             as package_piece_count,
    coalesce(round(cast(expected_shipping_costs as double precision) / ex.rate, 2), 0)                       as expected_shipping_costs_usd,                                                                      
    service_level,
    package_handler,
    destination_code,
    NULL                                                                                                     as destination_company,
    NULL                                                                                                     as destination_state_code,
    NULL                                                                                                     as destination_zip_code,
    coalesce(case when regexp_like(total_weight, '^[0-9]+(\.[0-9]+)?$') then cast(total_weight as number) 
        else null 
    end, 0)                                                                                                  as package_total_weight_kg, --todo-migration-test
    coalesce(case when regexp_like(total_weight, '^[0-9]+(\.[0-9]+)?$') then cast(total_weight as number) 
        else null 
    end, 0) / 0.45359237                                                                                     as package_total_weight_lb  -- Convert kg to pounds --todo-migration-test


    -- Timestamp Fields
    to_timestamp(replace(replace(started_at, 'T', ' '), 'Z', ''), 'yyyy-mm-dd hh24:mi:ss.ff')
                as logistics_operation_start_time, --todo-migration-test
    created_at                                                                                               as label_created_at,
    updated_at                                                                                               as logistics_operation_updated_at,
    extract(epoch from (label_created_at - logistics_operation_start_time))                                  as time_spent_in_seconds,

    -- Boolean Fields   
    {{ varchar_to_boolean('is_conforming') }}                                                                

from
    {{ source('int_retool', 'dhl_api_documents') }} as eu_logistics_operation
    left join {{ ref('exchange_rate_daily') }} as ex on eu_logistics_operation.expected_shipping_costs_currency = ex.currency_code_to and trunc(eu_logistics_operation.created_at) = trunc(ex.date)

union all

select
    'us'                                                                                                     as robowarehouse_region,
    md5(concat('us_', id))                                                                                   as logistics_operation_uuid,
    id                                                                                                       as logistics_operation_id,
    tracking_number,                                                                                           
    ordnum                                                                                                   as document_number,
    cast(piece_count as integer)                                                                             as package_piece_count,
    coalesce(round(cast(expected_shipping_costs as double precision), 2), 0)                                 as expected_shipping_costs_usd,
    service_level,
    package_handler,
    NULL                                                                                                     as destination_code,
    destination_company,
    destination_state_code,
    destination_zip                                                                                          as destination_zip_code,
    coalesce(case when regexp_like(total_weight, '^[0-9]+(\.[0-9]+)?$') then cast(total_weight as number) 
        else null 
    end, 0) * 0.45359237                                                                                     as package_total_weight_kg,  -- Convert to kg --todo-migration-test
    coalesce( 
    case 
        when regexp_like(total_weight, '^[0-9]+(\.[0-9]+)?$') then cast(total_weight as number) 
        else null 
    end, 0)                                                                                                  as package_total_weight_lb  -- Original weight in lb --todo-migration-test


    -- Timestamp Fields   
    to_timestamp(replace(replace(started_at, 'T', ' '), 'Z', ''), 'YYYY-MM-DD HH:MI:SS.MS')::timestamp       as logistics_operation_start_time,
    created_at                                                                                               as label_created_at,
    NULL                                                                                                     as logistics_operation_updated_at,
    extract(epoch from (label_created_at - logistics_operation_start_time))                                  as time_spent_in_seconds,

    -- Boolean Fields
    {{ varchar_to_boolean('is_conforming') }}                                                                                            
    
from
    {{ source('int_retool', 'ups_api_documents') }} as us_logistics_operation

union all 

select
    'uk'                                                                                                     as robowarehouse_region,
    md5(concat('uk_', id))                                                                                   as logistics_operation_uuid,
    id                                                                                                       as logistics_operation_id,
    tracking_number,
    ordnum                                                                                                   as document_number,
    cast(piece_count as integer)                                                                             as package_piece_count,
    coalesce(round(cast(expected_shipping_costs as double precision) / ex.rate, 2), 0)                       as expected_shipping_costs_usd,
    service_level,
    package_handler,
    destination_post_code                                                                                    as destination_code,
    destination_company,
    null                                                                                                     as destination_state_code,
    null                                                                                                     as destination_zip_code,
    coalesce(
    case when regexp_like(total_weight, '^[0-9]+(\.[0-9]+)?$') then cast(total_weight as number)
    else null end, 0)                                                                                        as package_total_weight_kg,  -- Original weight in kg --todo-migration-test
    coalesce(
    case when regexp_like(total_weight, '^[0-9]+(\.[0-9]+)?$') then cast(total_weight as number) * 2.20462
    else null end, 0)                                                                                        as package_total_weight_lb,  -- Convert to lb --todo-migration-test


    -- Timestamp Fields
    to_timestamp(replace(replace(started_at, 'T', ' '), 'Z', ''), 'YYYY-MM-DD HH:MI:SS.MS')::timestamp       as logistics_operation_start_time,
    created_at                                                                                               as label_created_at,
    null                                                                                                     as logistics_operation_updated_at,
    extract(epoch from (label_created_at - logistics_operation_start_time))                                  as time_spent_in_seconds,

    --Boolean Fields
    {{ varchar_to_boolean('is_conforming') }}

from 
    {{ source('int_retool', 'uk_ups_api_documents') }} as uk_logistics_operation
    left join {{ ref('exchange_rate_daily') }} as ex on uk_logistics_operation.expected_shipping_cost_currency = ex.currency_code_to and trunc(uk_logistics_operation.created_at) = trunc(ex.date)