select order_uuid,
       closed_at,
       sourced_at,
       technology_name,
       destination_market,
       subtotal_closed_amount_usd,
       subtotal_sourced_amount_usd - subtotal_sourced_cost_usd        as subtotal_sourced_precalculated_margin_usd,
       subtotal_sourced_amount_usd                                                                           
from  {{ ref('fact_orders') }}
where closed_at >= '2021-01-01'
