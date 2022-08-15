{{ config(  materialized='table',
            tags=["multirefresh"]
    ) }}


-- This table is created by combining supply's cnc orders table with 
-- several staging tables and other few secondary sources

-- Staging Tables:
-- Stg Orders Hubspot Table
-- Stg Orders Documents Table (Quotes & POs)
-- Stg Finance Table
-- Stg Logistics Table
-- Stg OTR Table (Depends on Documents & Logistics)
-- Stg Reviews Table
-- Stg Geo/Location Table
-- Stg Deal Stage Table
-- Stg Disputes Table

-- Aggregate Tables:
-- Agg Orders RDA Table
-- Agg Line Items Table
-- Agg Interactions Table

-- Other Sources:
-- Cancellation Reasons
-- Change Requests

select

    ------------- SOURCE: DATA LAKE ORDERS -----------
    -- Orders: IDs
    case when legacy_order_id is not null then orders.legacy_order_id::varchar
         when legacy_order_id is null then orders.uuid end                                 as order_uuid, -- Most drupal orders exists in supply but we want to keep their original ID
    orders.quote_uuid                                                                      as order_quote_uuid,
    orders.reorder_original_order_uuid,
    orders.billing_request_id                                                              as billing_id, -- This is the key used to indenitfy when the order was paid out and under which billing month

    -- Orders: Dates
    orders.created                                                                         as created_at,
    case
        when orders.promised_shipping_date > '2019-10-01'
            then orders.promised_shipping_date end                                         as promised_shipping_at_to_customer,
    orders.completed_at,

    -- Orders: Other Fields
    'supply'                                                                               as data_source,
    case when orders.legacy_order_id is not null then true else false end                  as is_legacy,

    -- Product Features
    orders.is_eligible_for_restriction,

    ---------- SOURCE: STG ORDERS HUBSPOT --------------

    -- HS Deals: Main Fields
    hs_deals.hubspot_amount_usd,
    hs_deals.hubspot_estimated_close_amount_usd,
    hs_deals.is_high_risk,
    hs_deals.rfq_priority,
    hs_deals.hubspot_pipeline,

    -- HS Deals: Foreign Fields
    hs_deals.hubspot_company_id,
    hs_deals.hubspot_company_name,
    hs_deals.hubspot_contact_id,
    hs_deals.hubspot_technology_id,
    hs_deals.hubspot_company_source,

    -- HS Deals: Dates
    hs_deals.hubspot_created_at,
    hs_deals.hubspot_closed_at,
    hs_deals.first_time_quote_sent_at,
    hs_deals.first_time_response_at,

    -- HS Deals: Lifecycle
    hs_deals.hubspot_dealstage_mapped,
    hs_deals.hubspot_dealstage_mapped_sort_index,
    hs_deals.in_review_reason,
    hs_deals.in_review_type,
    hs_deals.hubspot_closed_lost_reason,
    hs_deals.delay_liability,
    hs_deals.delay_status,

    -- TODO: create aggregate table for deal's employee/owners fields
    -- HS Deals: Owners
    hs_deals.bdr_owner_id,
    hs_deals.bdr_owner_name,
    hs_deals.bdr_owner_primary_team,
    hs_deals.customer_success_representative_name,
    hs_deals.partner_support_representative_name,
    hs_deals.mechanical_engineer_id,
    hs_deals.mechanical_engineer_name,
    hs_deals.hubspot_owner_id,
    hs_deals.hubspot_owner_name,
    hs_deals.hubspot_owner_primary_team,
    hs_deals.hubspot_purchasing_manager,
    hs_deals.hubspot_technical_review_owner,
    hs_deals.hubspot_sourcing_owner_id,
    hs_deals.hubspot_sourcing_owner_name,
    hs_deals.hubspot_owner_assigned_date,
    hs_deals.sales_lead_id,
    hs_deals.sales_lead_name,
    hs_deals.hubspot_im_project_manager_id,
    hs_deals.hubspot_im_project_manager_name,

    -- HS Deals: Properties Requested by Teams
    -- Check upstream model for details on the team
    hs_deals.rfq_type,
    hs_deals.review_outcome,
    hs_deals.is_target_price_met,
    hs_deals.is_target_lead_time_met,
    hs_deals.custom_approval,
    hs_deals.rejected_reason,
    hs_deals.im_deal_type,
    hs_deals.original_im_order_document_number,
    hs_deals.ctq_check,
    hs_deals.is_strategic,
    hs_deals.bdr_campaign,
    hs_deals.closing_probability,
    hs_deals.qc_inspection_result,
    hs_deals.qc_inspection_result_latest,
    hs_deals.in_country_qc_status,
    hs_deals.me_team_review_results,
    hs_deals.is_delayed_due_to_customs,

    ---------- SOURCE: STG ORDERS RDA --------------

    -- RDA: Auction Fields
    coalesce(rda.is_rda_sourced, false) as is_rda_sourced,
    rda.auction_uuid,
    rda.auction_status,
    rda.auction_created_at,
    rda.auction_started_at,
    rda.auction_finished_at,
    rda.auction_is_accepted_manually,
    rda.auction_is_reviewed_manually,
    rda.auction_support_ticket_opened_at,
    rda.auction_technology_id,
    rda.auction_document_number,
    rda.auction_is_cancelled_manually,
    rda.has_restricted_suppliers,
    rda.auction_cancelled_manually_at,

    -- RDA: Interaction Aggregates
    rda.number_of_rda_auctions,
    rda.number_of_supplier_auctions_assigned,
    rda.number_of_supplier_auctions_seen,
    rda.number_of_responses,
    rda.number_of_positive_responses,
    rda.number_of_countered_responses,
    rda.number_of_rejected_responses,
    rda.number_of_design_counterbids,
    rda.number_of_lead_time_counterbids,
    rda.number_of_price_counterbids,

    --RDA: Winning Bid Fields
    rda.winning_bid_uuid,
    rda.winning_bid_margin,
    rda.winning_bid_margin_usd,
    rda.winning_bid_margin_loss_usd,
    rda.has_winning_bid,
    rda.has_accepted_winning_bid,
    rda.has_restricted_winning_bid,
    rda.has_winning_bid_countered_on_price,
    rda.has_winning_bid_countered_on_lead_time,
    rda.has_winning_bid_countered_on_design,

    --RDA: Eligibility Sample (Product Feature a.k.a matching score)
    rda.number_of_eligible_suppliers,
    rda.number_of_eligible_preferred_suppliers,
    rda.number_of_eligible_local_suppliers,

    ---------- SOURCE: STG ORDERS RFQ --------------
    rfq.has_rfq,
    rfq.has_automatically_allocated_rfq,
    rfq.is_rfq_automatically_sourced,
    rfq.number_of_rfqs,
    rfq.number_of_suppliers_rfq_requests,
    rfq.number_of_suppliers_rfq_responded, 
    rfq.number_of_rfq_requests,
    rfq.number_of_rfq_responded,

    --------- SOURCE: STG ORDERS DOCUMENTS ---------

    --Documents: First Quote
    docs.quote_first_created_by_admin,
    docs.quote_first_splitted_from_quote_uuid,
    docs.is_splitted_from_order,
    docs.is_splitted_order,

    --Documents: Order Quote
    -- Active/Won Quote if submitted, else First
    docs.order_quote_document_number,
    docs.order_quote_status,
    docs.order_quote_created_at,
    docs.order_quote_submitted_at,
    docs.order_quote_finalised_at,
    docs.order_quote_lead_time as lead_time,
    docs.order_quote_lead_time_tier as lead_time_tier,
    docs.order_quote_is_cross_docking as is_cross_docking,
    docs.order_quote_requires_local_sourcing,

    --Documents: All Quotes
    docs.order_first_submitted_at,
    docs.number_of_quote_versions,
    docs.has_admin_created_quote,
    docs.has_manual_quote_review,

    --Documents: First Purchase Order
    docs.po_first_uuid,
    docs.subtotal_sourced_cost_usd,    
    docs.sourced_at,
    docs.is_sourced,

    --Documents: Active Purchase Order
    docs.po_active_uuid,
    docs.po_active_subtotal_cost_usd,
    docs.po_active_document_number,
    docs.po_active_company_entity,
    docs.po_active_support_ticket_id,
    docs.promised_shipping_at_by_supplier,

    --Documents: All Purchase Orders
    docs.number_of_purchase_orders,

    --Documents: Combined Fields
    docs.is_resourced,

    --------- SOURCE: STG ORDERS FINANCE ---------

    -- Finance: Stripe Fields
    finance.stripe_is_successful_payment,

    -- Finance: Netsuite Fields
    finance.payment_label,
    finance.order_remaining_amount as remaining_amount,
    finance.order_remaining_amount_usd as remaining_amount_usd,

    -- Finance: Fields from Combined Sources
    finance.is_auto_payment,
    finance.is_instant_payment,
    finance.payment_method,


    -------- SOURCE: STG ORDERS LOGISTICS --------

    -- Logistics: Base Fields
    logistics.number_of_shipments,
    logistics.number_of_batches,
    logistics.cross_dock_city,
    logistics.cross_dock_country,
    logistics.cross_dock_latitude,
    logistics.cross_dock_longitude,

    -- Logistics: Shipping Legs
    logistics.first_leg_carrier_name,
    logistics.first_leg_carrier_name_mapped,
    logistics.second_leg_carrier_name,
    logistics.second_leg_carrier_name_mapped,

    -- Logistics: Verification and Consistency Fields
    logistics.has_shipment_delivered_to_crossdock_date_consecutive,
    logistics.has_shipment_delivered_to_customer_date_consecutive,
    logistics.has_consistent_shipping_info,

    -- Logistics: Shipping Dates
    logistics.shipped_at as order_shipped_at, -- Prefix to avoid ambiguous field
    logistics.shipped_to_customer_at,
    logistics.shipped_from_cross_dock_at,

    -- Logistics: Delivery Dates
    logistics.delivered_at,
    logistics.full_delivered_at, -- Used for a definition
    logistics.derived_delivered_at,
    logistics.estimated_delivery_to_cross_dock_at,
    logistics.estimated_delivery_to_customer_at,
    logistics.delivered_to_cross_dock_at,

    -- Logistics: Estimates
    quotes.shipping_price_estimates,

    -------- SOURCE: STG OTR -----------
    -- Calculated based on cnc orders, and
    -- the stg tables of documents & logistics

    otr.is_shipped_on_time_by_supplier,
    otr.is_picked_up_on_time_from_supplier,
    otr.is_shipped_on_time_to_customer,
    otr.is_pick_up_on_time_to_customer,
    otr.shipping_to_customer_delay_days,
    otr.shipping_by_supplier_delay_days,
    -- Delay Notifications
    otr.has_delay_notifications,
    otr.number_of_delays,
    otr.has_delay_liability_supplier,
    otr.first_delay_created_at,

    -------- SOURCE: AGG ORDERS LINE ITEMS --------

    -- Quote
    qli.number_of_part_line_items,
    qli.number_of_materials,
    qli.number_of_processes,
    qli.total_quantity,
    qli.total_weight_grams,
    qli.total_bounding_box_volume_cm3,
    qli.total_volume_cm3,
    qli.number_of_expedited_shipping_line_items,
    qli.has_customer_note,
    qli.has_technical_drawings,
    qli.has_custom_material_subset,
    qli.has_custom_finish,
    qli.parts_amount_usd,
    qli.shipping_amount_usd,    
    qli.discount_cost_usd,
    qli.other_line_items_amount_usd as other_amount_usd,
    qli.line_item_technology_id,
    qli.line_item_process_id as process_id,
    qli.line_item_process_name as process_name,
    qli.parts_titles,

    -- Purchase Orders
    fpoli.parts_amount_usd as parts_cost_usd,
    fpoli.shipping_amount_usd as shipping_cost_usd,    
    fpoli.other_line_items_amount_usd as other_costs_usd,

    apoli.parts_amount_usd as po_active_parts_cost_usd,
    apoli.shipping_amount_usd as po_active_shipping_cost_usd,    
    apoli.other_line_items_amount_usd as po_active_other_costs_usd,

    ------ SOURCE: STG REVIEWS ---------
    -- Data from Technical Reviews

    reviews.has_technical_review,
    reviews.hubspot_first_technical_review_ongoing_at,
    reviews.hubspot_first_technical_review_completed_at,

    ------ SOURCE: STG GEO ------------
    -- Location data from customers,
    -- suppliers and company entity

    geo.destination_city,
    geo.destination_latitude,
    geo.destination_longitude,
    geo.destination_country,
    geo.destination_country_iso2,
    geo.destination_market,
    geo.destination_region,
    geo.destination_us_state,
    geo.contact_email_from_hubs,
    geo.company_entity,
    geo.origin_country,
    geo.origin_latitude,
    geo.origin_longitude,

    ------ SOURCE: STG DEALSTAGE ---------
    -- Combines data from order history events (supply),
    -- hubspot dealstage history (hubspot).

    -- Closing
    coalesce(dealstage.is_closed, false) as is_closed,
    dealstage.closed_at,

    -- Cancellation
    dealstage.cancelled_at,

    -- Completion
    dealstage.first_completed_at, -- Used for a definition

    -- Status
    dealstage.order_status,

    ------ SOURCE: STG INTERACTIONS ---------
    -- The stg table is derived from the aggregation of
    -- fact_interactions which combines the sources of
    -- freshdesk interactions and hubspot engagements.

    interactions.number_of_interactions,
    interactions.number_of_outgoing_emails,
    interactions.number_of_incoming_emails,

    ------ SOURCE: STG ORDER DISPUTES ---------
    -- Data from Disputes and Dispute Resolution

    -- Fields from Disputes Tables
    coalesce(disputes.is_quality_disputed,false) as is_quality_disputed,
    disputes.dispute_created_at,

    disputes.dispute_status,
    disputes.dispute_requested_outcome,
    disputes.dispute_type,

    -- Fields from Dispute Resolutions
    disputes.dispute_resolution_at,
    disputes.dispute_resolution_time_hours,
    disputes.first_dispute_resolution_type,

    ---------- SOURCE: INT SERVICE SUPPLY --------------
    -- Joins that are used to bring a few fields
    -- , they do not aggregate or compile data

    change_requests.freshdesk_ticket_id                                                    as change_request_freshdesk_ticket_id,
    change_requests.status                                                                 as change_request_status,
    case when change_requests.status is not null then true else false end                  as has_change_request,

    ---------- SOURCE: COMBINED FIELDS --------------
    -- Fields that are defined from two or more sources

    -- IDs
    coalesce(orders.hubspot_deal_id, hs_deals.hubspot_deal_id)                             as order_hubspot_deal_id, -- Prefix to avoid ambiguous field
    coalesce(orders.number, docs.order_quote_document_number)                              as document_number,

    -- Lifecycle:
    order_hubspot_deal_id is not null                                                      as exists_in_hubspot,
    order_quote_status = 'cart'                                                            as is_cart,
    case when order_quote_status = 'cart' then null else -- In June 2021 some carts started being created in HS
        coalesce(docs.order_first_submitted_at, hs_deals.hubspot_created_at) end           as submitted_at,
    submitted_at is not null                                                               as is_submitted,
    coalesce(cancellation_reasons.title, nullif(hs_deals.hubspot_cancellation_reason, '')) as cancellation_reason,
    coalesce(logistics.full_delivered_at, dealstage.first_completed_at) is not null        as is_recognized,
    least(case
          when orders.hubspot_deal_id in
                ('2934481798', '2920072973', '2914482547', '2770247355', 
                 '3033179401', '2410602207', '2966346046', '3020615482', 
                 '2975227287', '2887063884', '2950247669', '2901736923', 
                 '2860257553', '3021663769') then dealstage.first_completed_at
          when order_shipped_at > logistics.full_delivered_at 
          then dealstage.first_completed_at
          else logistics.full_delivered_at end, dealstage.first_completed_at)              as recognized_at, -- Let's think of a way to do this better :)

    -- Financial:
    coalesce(docs.order_quote_amount_usd, hs_deals.hubspot_amount_usd)                     as subtotal_amount_usd,
    case when is_closed then subtotal_amount_usd else 0 end                                as subtotal_closed_amount_usd,
    case when is_sourced then subtotal_amount_usd else 0 end                               as subtotal_sourced_amount_usd,

    -- Suppliers:
    coalesce(docs.po_active_supplier_id, rda.auction_supplier_id)                          as supplier_id,
    coalesce(docs.po_active_supplier_name, rda.auction_supplier_name)                      as supplier_name,
    coalesce(docs.po_active_supplier_address_id, rda.auction_supplier_address_id)          as supplier_address_id,

    -- Technology:
    coalesce(rda.auction_technology_id, qli.line_item_technology_id, hubspot_technology_id) as technology_id,
    coalesce(rda.auction_technology_name, qli.line_item_technology_name,
             hubspot_technology_name)                                                      as technology_name,

    -- Commission Related:
    case when hs_deals.hubspot_amount_usd - docs.order_quote_amount_usd - qli.shipping_amount_usd > 50 -- Threshold
            and interactions.has_svp_interaction is not true and is_closed is true 
            then true when is_closed is not true then null else false end                  as has_significant_amount_gap, 
    coalesce(interactions.has_svp_interaction or qli.has_svp_line_item,false)               as is_svp

from {{ ref('prep_supply_orders') }} as orders

    -- Staging
    left join {{ ref ('stg_orders_hubspot') }} as hs_deals on hs_deals.hubspot_deal_id = orders.hubspot_deal_id
    left join {{ ref ('stg_orders_documents') }} as docs on orders.uuid = docs.order_uuid
    left join {{ ref ('stg_orders_finance') }} as finance on orders.uuid = finance.order_uuid
    left join {{ ref ('stg_orders_logistics') }} as logistics on orders.uuid = logistics.order_uuid
    left join {{ ref ('stg_orders_otr') }} as otr on orders.uuid = otr.order_uuid
    left join {{ ref ('stg_orders_geo') }} as geo on orders.uuid = geo.order_uuid
    left join {{ ref ('stg_orders_dealstage') }} as dealstage on orders.uuid = dealstage.order_uuid
    left join {{ ref ('stg_orders_disputes') }} as disputes on orders.uuid = disputes.order_uuid

    -- Reporting
    left join {{ ref ('fact_discounts')}} as discounts on orders.uuid = discounts.order_uuid

    -- Aggregates
    left join {{ ref ('agg_orders_rda') }} as rda on orders.uuid = rda.order_uuid
    left join {{ ref ('agg_orders_rfq') }} as rfq on orders.uuid = rfq.order_uuid
    left join {{ ref ('agg_orders_technical_reviews') }} as reviews on orders.uuid = reviews.order_uuid
    left join {{ ref ('agg_orders_interactions')}} as interactions on orders.hubspot_deal_id = interactions.hubspot_deal_id
    left join {{ ref ('agg_line_items') }} as qli on orders.quote_uuid = qli.quote_uuid -- Agg Order-Quotes
    left join {{ ref ('agg_line_items') }} as fpoli on docs.po_first_uuid = fpoli.quote_uuid -- Agg First POs
    left join {{ ref ('agg_line_items') }} as apoli on docs.po_active_uuid = apoli.quote_uuid -- Agg Active POs        

    -- Data Lake
    left join {{ ref ('prep_supply_documents') }} as quotes on orders.quote_uuid = quotes.uuid

    -- Service Supply
    left join {{ source('int_service_supply', 'order_change_requests') }} as change_requests on orders.uuid = change_requests.order_uuid
    left join {{ source('int_service_supply', 'cancellation_reasons') }} as cancellation_reasons on orders.cancellation_reason_id = cancellation_reasons.id

where true
  and orders.legacy_order_id is null -- We take legacy orders from data_lake.legacy_orders table as source of truth in a later stage
  and coalesce (orders.hubspot_deal_id, -9) != 1062498043 -- Manufacturing agreement, orders were logged separately
