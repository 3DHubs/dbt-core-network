--JG sample code to generate business minutes between two timestamps. Load is relatively long, so we need to decide how to further implement.

with 

orders as (
    select * from {{ ref('fact_orders') }} where delivered_to_cross_dock_at >='2022-06-01'
),

final as (
    
    select 
        orders.order_uuid,
        order_hubspot_deal_id,
        cross_dock_city,
        case when cross_dock_city = 'Chicago' then  convert_timezone('America/Chicago', delivered_to_cross_dock_at) else
        convert_timezone('Europe/Amsterdam', delivered_to_cross_dock_at) end as delivered_to_cross_dock_at_local,
        case when cross_dock_city = 'Chicago' then  convert_timezone('America/Chicago', shipped_from_cross_dock_at) else
        convert_timezone('Europe/Amsterdam', shipped_from_cross_dock_at) end as shipped_from_cross_dock_at_local,
        {{ business_minutes_between('delivered_to_cross_dock_at_local', 'shipped_from_cross_dock_at_local') }} as business_minutes_at_cross_dock


    
    from orders   
    
)

select * from final