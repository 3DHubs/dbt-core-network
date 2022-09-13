select 
    order_uuid,
    hubspot_contact_id,
    closed_at,
    sourced_at,
    technology_name,
    destination_market,
    order_status,

    is_integration,
    integration_order_id, 
    integration_order_number, 
    integration_purchase_order_number,

    subtotal_closed_amount_usd,
    subtotal_sourced_amount_usd - subtotal_sourced_cost_usd        as subtotal_sourced_precalculated_margin_usd,
    subtotal_sourced_cost_usd,
    subtotal_sourced_amount_usd,

    --on time attributes
    is_shipped_on_time_by_supplier,
    is_shipped_on_time_to_customer,
    delay_liability,
    
    --sourcing attributes
    number_of_design_counterbids, 
    number_of_lead_time_counterbids,
    number_of_price_counterbids,
    has_winning_bid_countered_on_design, 
    has_winning_bid_countered_on_lead_time,
    has_winning_bid_countered_on_price                                                                           
from  {{ ref('fact_orders') }}
where closed_at >= '2019-01-01'
