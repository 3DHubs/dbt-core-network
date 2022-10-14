-- Data is relatively slow to generate, therefore in a derived table. Will be joined back into fact_orders in Looker.
{{ 
	config(materialized='incremental',
    tags=["slow_running"]
    )
}}
with 

orders as (
    select * from {{ ref('stg_orders_logistics') }} 
    where delivered_to_cross_dock_at >='2022-01-01'   
    and delivered_to_cross_dock_at is not null
    and shipped_from_cross_dock_at is not null


{% if is_incremental() %}

	and order_uuid not in (select distinct order_uuid from {{ this }})


{% endif %}
)

   select 
        orders.order_uuid,
        cross_dock_city,
        case when cross_dock_city = 'Chicago' then  convert_timezone('America/Chicago', delivered_to_cross_dock_at) else
        convert_timezone('Europe/Amsterdam', delivered_to_cross_dock_at) end as delivered_to_cross_dock_at_local,
        case when cross_dock_city = 'Chicago' then  convert_timezone('America/Chicago', shipped_from_cross_dock_at) else
        convert_timezone('Europe/Amsterdam', shipped_from_cross_dock_at) end as shipped_from_cross_dock_at_local,
         {{ business_minutes_between('delivered_to_cross_dock_at_local', 'shipped_from_cross_dock_at_local') }} as time_transit_at_cross_dock_business_minutes
    from orders   
