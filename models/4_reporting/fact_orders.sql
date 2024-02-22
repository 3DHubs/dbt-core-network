
--   _____ _    ____ _____    ___  ____  ____  _____ ____  ____  
--  |  ___/ \  / ___|_   _|  / _ \|  _ \|  _ \| ____|  _ \/ ___| 
--  | |_ / _ \| |     | |   | | | | |_) | | | |  _| | |_) \___ \ 
--  |  _/ ___ \ |___  | |   | |_| |  _ <| |_| | |___|  _ < ___) |
--  |_|/_/   \_\____| |_|    \___/|_| \_\____/|_____|_| \_\____/ 
                                                              
{{ config(
    tags=["multirefresh"]
) }}

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
document_number,
orders.project_uuid,

-- Foreign Keys
order_quote_uuid,
process_id,
technology_id,
orders.hubspot_company_id,
orders.hubspot_contact_id,
coalesce(md5(concat('company', orders.hubspot_company_id)),md5(concat('contact', orders.hubspot_contact_id))) as client_id,
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
hubspot_im_project_manager_id,
orders.billing_id,

-- General Attributes
process_name,
technology_name,
lead_time,
sourcing_window,
price_multiplier,
has_significant_amount_gap, 
is_svp,
data_source,
is_legacy, 


-- External Attributes
is_papi_integration,
case when is_integration_contact then true else is_integration_tmp end as is_integration, -- including indirect integration revenue
case when is_integration_tmp then 'direct'
     when is_integration then 'indirect'  end as integration_order_type,
case when integration_platform_type is null and integration_order_type = 'indirect' then 'indirect' else integration_platform_type end as integration_platform_type,
integration_order_id, 
integration_quote_id,
integration_order_number, 
integration_purchase_order_number,
integration_user_id,
integration_utm_content,
number_of_orders_per_integration_order,
is_multi_line_papi_integration,


-- Lifecycle Dates
created_at, -- Upload/cart
submitted_at, -- Quote request
hubspot_owner_assigned_date,
time_in_stage_new_business_minutes,
time_in_stage_dfm_minutes,
first_time_quote_sent_at,
first_time_response_at,
hubspot_first_technical_review_ongoing_at,
hubspot_first_technical_review_completed_at,
closed_at, -- When clients pays, also known as won
hubspot_closed_at,
cancelled_at,
auction_created_at, -- When the auctions enters the RDA
auction_started_at,
auction_finished_at,
sourced_at,
order_shipped_at,
localized_order_shipped_at,
shipped_to_customer_at,
promised_shipping_at_to_customer, -- From order quote
localized_promised_shipping_at_to_customer,
promised_shipping_at_by_supplier, -- From active PO
original_shipping_at_by_supplier, -- In case of winning counterbid lead time different
localized_promised_shipping_at_by_supplier,
estimated_delivery_to_cross_dock_at,
delivered_to_cross_dock_at,
shipped_from_cross_dock_at,
shipment_label_created_at, -- old method for shipped_at

time_transit_at_cross_dock_business_minutes,
estimated_delivery_to_customer_at,
delivered_at,
derived_delivered_at,
po_active_finalized_at,
recognized_at,
completed_at,
dispute_created_at,
dispute_resolution_at,

-- Lifecycle
is_cart,
is_closed,
is_sourced,
order_status,
exists_in_hubspot,
is_resourced,
cancellation_reason,
cancellation_reason_mapped,
number_of_cancellations,
is_recognized,
is_quality_disputed,

-- Purchase Orders
number_of_purchase_orders,
po_active_document_number,

-- Quotes
order_quote_document_number,
order_quote_status,
order_quote_is_admin,
rfq_quote_note,
rfq_quote_application,
rfq_quote_delivered_by,
quote_first_created_by_admin,
number_of_quote_versions,
has_admin_created_quote,
has_manual_quote_review,
has_request_review,


-- Company Attributes
hubspot_company_name,
hubspot_company_source,
pl_cross_sell_company_name,
agg.became_opportunity_at_company,
agg.became_customer_at_company,
agg.created_order_is_from_new_company,
agg.closed_order_is_from_new_customer_company,
agg.closed_order_number_company,
agg.closed_project_number_company,
agg.days_from_previous_closed_order_company,
agg.first_bdr_owner_at_company,

-- Contact Attributes
agg.became_opportunity_at_contact,
agg.became_customer_at_contact,
agg.created_order_is_from_new_contact,
agg.closed_order_is_from_new_customer_contact,
agg.closed_order_number_contact,
agg.days_from_previous_closed_order_contact,

-- Client Attributes
agg.became_opportunity_at_client,
agg.became_customer_at_client,
agg.created_order_is_from_new_client,
agg.closed_order_is_from_new_customer_client,
agg.closed_order_number_client,

-- Plaform Attributes
orders.is_anonymous_cart,
orders.platform_user_id,
agg.number_of_carts_without_closed_carts_platform_user_id,

-- Project Attributes 
agg.project_amount_usd,
agg.project_order_count,

-- Amounts Quote (USD):
parts_amount_usd, -- "order_quote_" fields are derived from line items
shipping_amount_usd,    
discount_cost_usd,
other_amount_usd,
subtotal_amount_usd, -- Value derived from the quotes table, closing and sourcing should vary due to exchange rates 
subtotal_closed_amount_usd,
subtotal_sourced_amount_usd,
hubspot_amount_usd,
hubspot_estimated_close_amount_usd,
shipping_cost_usd,

-- Amounts PO (USD):
subtotal_sourced_cost_usd,
po_first_sourced_cost_usd,
parts_cost_usd,
po_first_shipping_cost_usd,
other_costs_usd,
po_active_subtotal_cost_usd,
po_active_parts_cost_usd,
po_active_shipping_cost_usd,
po_active_other_costs_usd,
subtotal_po_cost_usd,

-- Amounts CM1 (USD)
cogs_amount_usd,
recognized_revenue_amount_usd,
contribution_margin_amount_usd,

-- Geo/Location
cross_dock_city,
cross_dock_country,
cross_dock_latitude,
cross_dock_longitude,
destination_company_name,
destination_city,
destination_latitude,
destination_longitude,
destination_country,
destination_country_iso2,
destination_market,
destination_region,
destination_sub_region,
destination_us_state,
origin_country,
origin_latitude,
origin_longitude,
origin_market,
origin_region,

-- Hubspot Attributes
is_delayed_due_to_customs,
me_team_review_results,
hubspot_pipeline,
is_high_risk,
rfq_priority,
hubspot_dealstage_mapped,
hubspot_dealstage_mapped_sort_index,
hubspot_closed_lost_reason,
rfq_type,
review_outcome,
is_target_price_met,
is_target_lead_time_met,
custom_approval,
rejected_reason,
im_deal_type,
im_post_sales_value_score,
im_post_sales_concerning_actions,
original_im_order_document_number,
ctq_check,
is_sales_priced,
is_strategic,
is_priority_deal,
closing_probability,
qc_inspection_result,
qc_inspection_result_latest,
in_country_qc_status,
pl_cross_sell_channel,
mp_concerning_actions,


-- Hubspot Owners
hubspot_owner_name,
hubspot_owner_primary_team,
bdr_owner_name,
bdr_owner_primary_team,
customer_success_representative_name,
partner_support_representative_name,
mechanical_engineer_name,
hubspot_purchasing_manager,
hubspot_technical_review_owner,
hubspot_sourcing_owner_name,
hubspot_im_project_manager_name,
sales_lead_name,
pl_sales_rep_name,
pl_sales_rep_manager_name,
pl_business_development_manager_name,

-- Line Items
number_of_part_line_items,
number_of_materials,
number_of_processes,
total_quantity,
total_weight_grams,
total_bounding_box_volume_cm3,
total_volume_cm3,
has_customer_note,
has_technical_drawings,
has_custom_material_subset,
has_custom_finish,
parts_max_depth_cm,
parts_max_heigth_cm,
parts_max_width_cm,
greatest(parts_max_depth_cm, parts_max_heigth_cm, parts_max_width_cm ) as max_part_size,
parts_titles,
is_vqced,

-- Generic Auction fields (RDA + RFQ)
has_winning_bid_any_auction,
number_of_auctions,
number_of_auction_cancellations,

-- RDA (Reverse Dutch Auction)
auction_document_number,
auction_status,
auction_is_accepted_manually,
auction_is_reviewed_manually,
auction_is_cancelled_manually,
auction_cancelled_manually_at,
auction_support_ticket_opened_at,
is_rda_sourced,
is_first_auction_rda_sourced,
is_eligible_for_restriction,
has_restricted_suppliers,
number_of_rda_auctions,
number_of_eligible_suppliers,
number_of_eligible_preferred_suppliers,
number_of_eligible_local_suppliers,
number_of_supplier_auctions_assigned,
number_of_supplier_auctions_seen,
number_of_responses,
number_of_positive_responses,
number_of_countered_responses,
number_of_rejected_responses,
number_of_design_counterbids,
number_of_lead_time_counterbids,
number_of_price_counterbids,
has_winning_bid,
winning_bid_margin,
winning_bid_margin_usd,
winning_bid_margin_loss_usd,
winning_shipping_estimate_amount_usd,
winning_l1_shipping_margin_amount_usd,
l1_shipping_estimate_source,
has_accepted_winning_bid,
has_restricted_winning_bid,
has_winning_bid_countered_on_price,
has_winning_bid_countered_on_lead_time,
has_winning_bid_countered_on_design,

-- RFQ (Request for Quotation)
has_rfq,
has_automatically_allocated_rfq,
is_rfq_automatically_sourced,
number_of_rfqs,
number_of_suppliers_rfq_requests,
number_of_suppliers_rfq_responded, 
number_of_rfq_requests,
number_of_rfq_responded,

-- Finance
stripe_is_successful_payment,
po_active_company_entity,
is_auto_payment,
is_instant_payment,
payment_method,
company_entity, -- From the order quote
payment_label,
remaining_amount,
remaining_amount_usd,
--Finance related exchange rates
exchange_rate_at_closing,
exchange_rate_at_sourcing,

-- Logistics
number_of_shipments,
is_cross_docking,
is_eligible_for_cross_docking,
is_local_sourcing,
is_eligible_for_local_sourcing,
number_of_batches,
has_consistent_shipping_info,
is_hubs_arranged_direct_shipping,
chargeable_shipping_weight_estimate_kg,
is_logistics_shipping_quote_used,

-- Logistics: First leg and second leg carriers
first_leg_carrier_name,
first_leg_carrier_name_mapped,
second_leg_carrier_name,
second_leg_carrier_name_mapped,

-- Logistics: Estimates
estimated_l1_customs_amount_usd,
estimated_l2_customs_amount_usd,

-- On Time Rate
first_leg_buffer_value,
is_shipped_on_time_by_supplier,
is_shipped_on_time_to_customer,
shipping_to_customer_delay_days,
shipping_by_supplier_delay_days,


-- RFQ & Technical Review
in_review_reason,
in_review_type,
has_technical_review,

-- Delays
delay_liability,
delay_reason,
delay_status,
has_delay_notifications,
number_of_delays,
has_delay_liability_supplier,
first_delay_created_at,
delay_probability,

-- Disputes
dispute_status,
dispute_requested_outcome,
dispute_type,
dispute_resolution_time_hours,
first_dispute_resolution_type,

-- Interactions
number_of_interactions,
number_of_outgoing_emails,
number_of_incoming_emails,
number_of_interactions_fd,
number_of_outgoing_emails_fd,
number_of_incoming_emails_fd,
number_of_notes_fd,

-- Freshdesk
change_request_status,
has_change_request,
po_active_support_ticket_id,

-- Original Orders
coalesce(original.is_reorder, false) as is_reorder,
original.original_order_created_at,
original.original_order_closed_at,
original.original_order_lead_time,
original.original_order_amount_usd,
original.original_order_quantity,
original.original_order_number_of_part_line_items,
original.original_order_parts_titles,

-- Splitted Orders (Competitiveness Feature) 
quote_first_splitted_from_quote_uuid,
is_splitted_from_order,
is_splitted_order,

-- Special Projects
logistics_co2_emissions_g,
travel_distance_km,
manufacturing_co2_emissions_g,
procurement_co2_emissions_g

from complete_orders as orders
left join {{ ref('agg_orders') }} as agg on agg.order_uuid = orders.order_uuid
left join {{ ref('agg_orders_cm1') }} as agg_cm1 on agg_cm1.order_uuid = orders.order_uuid
left join {{ ref('stg_fact_reorders') }} as original on original.reorder_order_uuid = orders.order_uuid
left join {{ ref('stg_order_greenhubs') }} as gh on orders.order_uuid = gh.order_uuid
where coalesce(number_of_carts_without_closed_carts_platform_user_id,1)  < 30 or subtotal_sourced_amount_usd >0 --removal of bot carts
