select distinct fact_orders.hubspot_company_id,
       sum(case
               when fact_orders.is_closed and agg_orders.is_new_customer_company then order_closed_amount_usd
               end)
       over ( partition by fact_orders.hubspot_company_id) as new_customer_order_closed_sales_usd,
       sum(case
               when fact_orders.is_closed and agg_orders.is_new_customer_company then (fact_orders.order_sourced_amount_usd - fact_orders.sourced_cost_usd)
               end)
       over (partition by fact_orders.hubspot_company_id)  as new_customer_precalc_margin_usd

from {{ ref('fact_orders') }}
        inner join {{ ref('agg_orders') }} on fact_orders.order_uuid = agg_orders.order_uuid