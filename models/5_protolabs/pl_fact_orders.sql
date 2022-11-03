{{ config(
    tags=["multirefresh"]
) }}

select 
    order_uuid,
    document_number,
    hubspot_contact_id,
    created_at, 
    submitted_at,
    closed_at,
    sourced_at,
    technology_name,
    destination_market,
    order_status,
    lead_time,
    hubspot_owner_name,

    is_papi_integration,
    is_integration_type,
    integration_order_id, 
    integration_order_number, 
    integration_purchase_order_number,
    integration_user_id,
    integration_utm_content,

    subtotal_closed_amount_usd,
    subtotal_sourced_amount_usd - subtotal_sourced_cost_usd        as subtotal_sourced_precalculated_margin_usd,
    subtotal_sourced_cost_usd,
    subtotal_sourced_amount_usd,

    --on time attributes
    promised_shipping_at_by_supplier,
    is_shipped_on_time_by_supplier,
    promised_shipping_at_to_customer,
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
where created_at >= '2019-01-01'
