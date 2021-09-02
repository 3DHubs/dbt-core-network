
--   _____ _    ____ _____    ___  ____  ____  _____ ____  ____  
--  |  ___/ \  / ___|_   _|  / _ \|  _ \|  _ \| ____|  _ \/ ___| 
--  | |_ / _ \| |     | |   | | | | |_) | | | |  _| | |_) \___ \ 
--  |  _/ ___ \ |___  | |   | |_| |  _ <| |_| | |___|  _ < ___) |
--  |_|/_/   \_\____| |_|    \___/|_| \_\____/|_____|_| \_\____/ 
                                                              

with complete_orders as (

-- This unions the fact_orders table together with the missing_orders table which is a static table
-- that contains deals from both Drupal and Hubspot that are not found in service supply (~9K).

  {{ dbt_utils.union_relations(
    relations=[ref('stg_fact_orders'), source('data_lake', 'legacy_orders')]
) }}

-- The DBT union relations package unions tables even when they have different widths and column orders

)

select

-- Primary Key
orders.order_uuid,
order_hubspot_deal_id,
order_document_number,

-- Foreign Keys
order_quote_uuid,
process_id,
order_technology_id,
orders.hubspot_company_id,
orders.hubspot_contact_id,
supplier_id,
po_active_uuid,
auction_uuid,
winning_bid_uuid,
reorder_original_order_uuid,
change_request_freshdesk_ticket_id,
bdr_owner_id,
mechanical_engineer_id,
hubspot_owner_id,
hubspot_sourcing_owner_id,

-- Main Attributes
order_process_name,
order_technology_name,
order_lead_time,
order_lead_time_tier,
is_expedited_shipping,

-- Lifecycle Dates
order_created_at, -- Upload/cart
order_submitted_at, -- Quote request
hubspot_owner_assigned_at,
first_time_quote_sent_at,
first_time_response_at,
supply_first_review_submitted_at,
hubspot_first_review_completed_at,
supply_last_review_completed_at,
order_closed_at, -- When clients pays, also known as won
hubspot_closed_at,
order_cancelled_at,
auction_created_at, -- When the auctions enters the RDA
auction_finished_at,
order_sourced_at,
first_delay_submitted_at,
order_shipped_at,
order_promised_shipping_at_to_customer, -- From order quote
order_promised_shipping_at_by_supplier, -- From active PO
estimated_delivery_to_cross_dock,
delivered_to_cross_dock_at,
shipped_from_cross_dock_at,
estimated_delivery_to_customer,
order_delivered_at,
derived_delivered_at,
full_delivered_at,
order_recognized_at,
order_completed_at,
order_first_completed_at,
order_last_completed_at,
dispute_created_at,
dispute_resolution_at,

-- Lifecycle
order_is_cart,
order_is_closed,
order_is_sourced,
order_status,
order_exists_in_hubspot,
order_is_resourced,
qc_inspection_result,
cancellation_reason,
order_is_recognized,
order_is_disputed,

-- Purchase Orders
number_of_purchase_orders,
po_active_document_number,

-- Quotes
order_quote_document_number,
order_quote_status,
quote_first_created_by_admin,
quote_first_has_part_without_automatic_pricing,
number_of_quote_versions,
number_of_quote_versions_by_admin,
has_admin_created_quote,
has_manual_quote_review,

-- Company Attributes
hubspot_company_name,
is_hubspot_strategic_company,
hubspot_company_source,

-- Contact Attributes
contact_email_from_hubs,

-- Supplier Attributes
supplier_name,

-- Amounts (USD)
order_amount_usd,
order_closed_amount_usd,
order_sourced_amount_usd,
sourced_cost_usd, -- From First PO
shipping_amount_usd, -- From Line Items
po_first_shipping_usd,
po_active_amount_usd,
po_active_shipping_usd,
hubspot_amount_usd,
hubspot_estimated_close_amount_usd,

-- Amounts Agg (USD)
order_cogs_amount_usd,
order_recognized_revenue_amount_usd,
order_margin_amount_usd,

-- Geo/Location
cross_dock_city,
cross_dock_country,
destination_city,
destination_latitude,
destination_longitude,
destination_country,
destination_country_iso2,
destination_market,
destination_region,
destination_us_state,
origin_country,
origin_latitude,
origin_longitude,

-- Hubspot Attributes
hubspot_pipeline,
hubspot_deal_category,
is_hubspot_strategic_deal,
is_hubspot_high_risk,
hubspot_dealstage_mapped,
hubspot_dealstage_mapped_sort_index,
hubspot_closed_lost_reason,

-- Hubspot Owners
hubspot_owner_name,
hubspot_owner_primary_team_name,
bdr_owner_name,
bdr_owner_primary_team_name,
customer_success_representative_name,
partner_support_representative_name,
mechanical_engineer_name,
hubspot_purchasing_manager,
hubspot_technical_review_owner,
hubspot_sourcing_owner_name,

-- Line Items
number_of_part_line_items,
number_of_materials,
number_of_processes,
order_total_quantity,
order_total_weight_grams,
order_total_bounding_box_volume_cm3,
order_total_volume_cm3,
number_of_expedited_shipping_line_items,
has_customer_note,
has_exceeded_standard_tolerances,
has_technical_drawings,
has_custom_material_subset,
has_custom_finish,

-- RDA (Reverse Dutch Auction)
auction_document_number,
auction_status,
auction_is_accepted_manually,
auction_is_reviewed_manually,
auction_is_cancelled_manually,
auction_cancelled_manually_at,
number_of_suppliers_assigned,
number_of_bids,
number_of_counterbids,
number_of_rejected_bids,
number_of_design_counterbids,
number_of_lead_time_counterbids,
number_of_price_counterbids,
order_has_winning_bid,
order_has_accepted_winning_bid,
order_has_winning_bid_countered_on_price,
order_has_winning_bid_countered_on_design,
order_is_rda_sourced,

-- Finance
stripe_transaction_created_at,
stripe_is_successful_payment,
po_active_company_entity,
is_auto_payment,
is_instant_payment,
payment_method,
company_entity, -- From the order quote

-- Logistics
number_of_shipments,
order_quote_is_cross_docking,
number_of_packages,
has_consistent_shipping_info,

-- On Time Rate
is_shipped_on_time_by_supplier,
is_shipped_on_time_to_customer,
shipping_to_customer_delay_days,
shipping_by_supplier_delay_days,

-- RFQ & Technical Review
number_of_technical_reviews,
in_review_reason,
in_review_type,
order_has_technical_review,
order_has_rfq,
number_of_rfq_requests,
number_of_rfq_responded,

-- Delays
delay_reason,
delay_liability,
delay_status,

-- Disputes
dispute_liability,
dispute_outcome,
dispute_reason,
dispute_status,
dispute_requested_outcome,
dispute_type,
dispute_resolution_time_hours,
first_dispute_resolution_type,

-- Interactions
order_number_of_interactions,
order_number_of_outgoing_emails,
order_number_of_incoming_emails,

-- Freshdesk
change_request_status,
has_change_request,

-- Other Attributes
order_is_underquoted,
order_is_svp,
order_data_source,
order_is_legacy

from complete_orders as orders
left join {{ ref('agg_orders') }} as agg on agg.order_uuid = orders.order_uuid
left join {{ ref('agg_orders_cm1') }} as agg_cm1 on agg_cm1.order_uuid = orders.order_uuid
where true
-- Let's try including carts and filtering them out in Looker
-- and (order_quote_status = 'cart') is not true
