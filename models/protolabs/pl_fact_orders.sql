select closed_at::date as closed_date,
       sourced_at::date as sourced_date,
       technology_name,
       destination_market,
       coalesce(sum(subtotal_closed_amount_usd), 0)                                     as total_closed_amount_usd,
       coalesce(sum((subtotal_sourced_amount_usd - subtotal_sourced_cost_usd)), 0)               as total_precalculated_margin_usd,
       coalesce(sum(subtotal_sourced_amount_usd), 0)                                    as total_sourced_amount_usd,
       count(case when fact_orders.is_closed then fact_orders.order_uuid else NULL end) as number_of_closed_orders
from  {{ ref('fact_orders') }}
where closed_at >= '2021-01-01'
group by 1, 2, 3, 4