select distinct orders.hubspot_company_id,
       sum(case
               when orders.order_is_closed and agg_orders.is_new_customer_company then order_closed_amount_usd
               end)
       over ( partition by orders.hubspot_company_id) as new_customer_order_closed_sales_usd,
       sum(case
               when orders.order_is_closed and agg_orders.is_new_customer_company then (orders.order_sourced_amount_usd - orders.sourced_cost_usd)
               end)
       over (partition by orders.hubspot_company_id)  as new_customer_precalc_margin_usd

from {{ ref('stg_fact_orders') }} as orders
        inner join {{ ref('agg_orders') }} on orders.order_uuid = agg_orders.order_uuid