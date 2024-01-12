-- Financial recognition logic
select
    orders.uuid as order_uuid,
    logistics.shipped_at as order_shipped_at,
    dealstage.closed_at,
    dealstage.first_completed_at,  -- Used for a definition
    coalesce(logistics.full_delivered_at, dealstage.first_completed_at)
    is not null as is_recognized,
    coalesce(sfrle.recognized_at,  
    least(
    case
            when order_shipped_at > logistics.full_delivered_at then dealstage.first_completed_at
            else logistics.full_delivered_at
        end,
        dealstage.first_completed_at
    )) as recognized_at
from {{ ref('prep_supply_orders') }} as orders
left join
    {{ ref('stg_orders_logistics') }} as logistics on orders.uuid = logistics.order_uuid
left join
    {{ ref('stg_orders_dealstage') }} as dealstage on orders.uuid = dealstage.order_uuid
left join
    {{ ref('seed_financial_recognition_logic_exceptions') }} as sfrle
    on orders.uuid = sfrle.order_uuid
