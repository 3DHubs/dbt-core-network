{{ config(
    tags=["multirefresh"]
) }}

select 
    order_uuid,
    document_number,
    order_hubspot_deal_id as hubspot_deal_id,
    hubspot_contact_id,
    created_at,
    submitted_at,
    closed_at,
    sourced_at,
    delivered_at,
    derived_delivered_at,
    first_time_quote_sent_at,
    technology_name,
    process_name,
    destination_region,
    destination_sub_region,
    destination_market,
    destination_company_name,
    destination_us_state,
    order_status,
    im_deal_type,
    lead_time,
    hubspot_owner_name,
    pl_sales_rep_name,
    pl_sales_rep_manager_name, 
    pl_cross_sell_company_name,
    pl_cross_sell_channel,
    pl_business_development_manager_name,
    hubspot_network_sales_specialist_name,
    hubspot_company_owner_name,

    is_itar, 
    is_integration,
    is_papi_integration,
    integration_platform_type,
    integration_order_id,
    integration_quote_id,
    integration_order_number,
    integration_purchase_order_number,
    integration_user_id,
    integration_utm_content,
    integration_order_type,

    subtotal_closed_amount_usd,
    subtotal_sourced_amount_usd - subtotal_sourced_cost_usd        as subtotal_sourced_precalculated_margin_usd,
    subtotal_sourced_cost_usd,
    subtotal_sourced_amount_usd,
    shipping_amount_usd,
    hubspot_estimated_close_amount_usd,
    subtotal_amount_usd,

    --on time attributes
    order_shipped_at,
    shipped_to_customer_at,
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
    has_winning_bid_countered_on_price,
    has_rfq,
    is_first_auction_rda_sourced,

    --hubspot attributes
    review_outcome,
    utm_campaign,
    utm_content,
    utm_source,
    utm_term,
    utm_medium,
    utm_campaign_name,
    last_traffic_source,

    -- Plaform Attributes
    is_anonymous_cart

from  {{ ref('fact_orders') }}
where created_at >= '2019-01-01'
