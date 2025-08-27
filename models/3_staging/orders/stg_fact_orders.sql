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
-- Stg Logistics Business Hours Table
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
    case
        when legacy_order_id is not null then orders.legacy_order_id::varchar
        when legacy_order_id is null then orders.uuid
    end                                                                                          as order_uuid, -- Most drupal orders exists in supply but we want to keep their original ID
    orders.quote_uuid                                                                            as order_quote_uuid,
    coalesce(orders.reorder_original_order_uuid,orders.reorder_with_same_mp_order_uuid)          as reorder_original_order_uuid,
    orders.billing_request_id                                                                    as billing_id, -- This is the key used to indenitfy when the order was paid out and under which billing month
    orders.cancellation_reason_id,
    case
        when
            dealstage.closed_at is not null
            then md5(concat(concat('project', coalesce(hs_deals.hubspot_contact_id, users.hubspot_contact_id)), dealstage.closed_at::date))
    end                                                                                          as project_uuid,

    -- Orders: Dates
    orders.created                                                                               as created_at,
    case
        when orders.promised_shipping_date > '2019-10-01'
            then orders.promised_shipping_date
    end                                                                                          as promised_shipping_at_to_customer,
    convert_timezone(
        destination_timezone, promised_shipping_at_to_customer
    )                                                                                            as localized_promised_shipping_at_to_customer,
    orders.completed_at,

    -- Orders: Other Fields
    'supply'                                                                                     as data_source,
    coalesce (orders.legacy_order_id is not null, false)                                         as is_legacy,
    orders.order_change_request_freshdesk_ticket_id                                              as change_request_freshdesk_ticket_id,
    orders.order_change_request_status                                                           as change_request_status,
    coalesce (change_request_status is not null, false)                                          as has_change_request,
    case when supplier_id = 467 then true else false end                                         as is_itar, 

    -- Product Features
    orders.is_eligible_for_restriction,
    case 
        when orders.reorder_with_same_mp_order_uuid is not null then 'with same mp' 
        when orders.reorder_original_order_uuid is not null then 'with any mp'                 
    end                                                                                          as reorder_type,
 

    -- Platform data
    users.platform_user_id,
    coalesce (length(users.platform_user_id) > 16, false)                                        as is_anonymous_cart,

    ---------- SOURCE: SUPPLY EXTERNAL ORDERS --------------

    -- External Orders: Main fields
    coalesce(integration.is_papi_integration, false)                                             as is_papi_integration,
    integration.integration_platform_type,
    integration.integration_order_id,
    integration.integration_quote_id,
    integration.integration_order_number,
    integration.integration_purchase_order_number,
    integration.integration_user_id,
    integration.integration_utm_content, -- JG may be removed after campaign 2022-12-01 PL shallow quicklink
    integration.number_of_orders_per_integration_order,
    integration.is_multi_line_papi_integration,



    ---------- SOURCE: STG ORDERS HUBSPOT --------------

    -- HS Deals: Main Fields
    hs_deals.hubspot_amount_usd,
    hs_deals.hubspot_estimated_close_amount_usd,
    hs_deals.is_high_risk,
    hs_deals.rfq_priority,
    hs_deals.hubspot_pipeline,

    -- HS Deals: Foreign Fields
    coalesce(hs_deals.hubspot_company_id, users.hubspot_company_id)                              as hubspot_company_id, -- for carts falling back on user link
    hs_deals.hubspot_company_name,
    hs_deals.pl_cross_sell_company_name,
    hs_deals.is_integration_mql_contact,
    coalesce(hs_deals.hubspot_contact_id, users.hubspot_contact_id)                              as hubspot_contact_id,
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
    hs_deals.delay_reason,
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
    hs_deals.mechanical_engineer_deal_buddy_name,
    hs_deals.hubspot_paid_sales_rep_id,
    hs_deals.hubspot_paid_sales_rep_name,
    hs_deals.hubspot_owner_id,
    hs_deals.hubspot_owner_name,
    hs_deals.hubspot_owner_primary_team,
    hs_deals.hubspot_purchasing_manager,
    hs_deals.hubspot_technical_review_owner,
    hs_deals.hubspot_sourcing_owner_id,
    hs_deals.hubspot_sourcing_owner_name,
    hs_deals.hubspot_owner_assigned_date,
    hs_deals.hubspot_quality_resolution_specialist_id,
    hs_deals.hubspot_quality_resolution_specialist_name,
    hs_deals.hubspot_technical_program_manager_id,
    hs_deals.hubspot_technical_program_manager_name,
    hs_deals.sales_lead_id,
    hs_deals.sales_lead_name,
    hs_deals.sales_support_id,
    hs_deals.sales_support_name,
    hs_deals.hubspot_im_project_manager_id,
    hs_deals.hubspot_im_project_manager_name,
    hs_deals.pl_sales_rep_name,
    hs_deals.pl_sales_rep_manager_name,
    hs_deals.pl_business_development_manager_name,
    hs_deals.hubspot_network_sales_specialist_name,
    hs_deals.hubspot_company_owner_name,

    -- HS Deals: Properties Requested by Teams
    -- Check upstream model for details on the team
    hs_deals.rfq_type,
    hs_deals.review_outcome,
    hs_deals.is_sales_priced,
    hs_deals.is_target_price_met,
    hs_deals.is_target_lead_time_met,
    hs_deals.custom_approval,
    hs_deals.rejected_reason,
    hs_deals.im_deal_type,
    hs_deals.im_post_sales_value_score,
    hs_deals.im_post_sales_concerning_actions,
    hs_deals.original_im_order_document_number,
    hs_deals.ctq_check,
    hs_deals.is_strategic,
    hs_deals.is_priority_deal,
    hs_deals.closing_probability,
    hs_deals.qc_inspection_result,
    hs_deals.qc_inspection_result_latest,
    hs_deals.in_country_qc_status,
    hs_deals.me_team_review_results,
    hs_deals.is_delayed_due_to_customs,
    hs_deals.is_hubs_arranged_direct_shipping,
    hs_deals.mp_concerning_actions,
    hs_deals.is_logistics_shipping_quote_used,
    hs_deals.is_manually_resourced,
    hs_deals.is_production_rfq,
    hs_deals.resourced_deal_original_order_number,
    case
        when hs_deals.hubspot_pl_cross_sell_channel is not null then hs_deals.hubspot_pl_cross_sell_channel
        when regexp_like(lower(hs_deals.hubspot_company_name), 'protolabs') then 'Twin-Win' --todo-migration-test
        when pl_sales_rep_name is not null then 'Twin-Win'
    end                                                                                          as pl_cross_sell_channel,
    coalesce(integration_platform_type is not null or pl_cross_sell_channel is not null, false)  as is_integration_tmp,
    hs_deals.hubspot_signed_customer_quote_pdf_link,
    hs_deals.why_still_in_production,

    -- HS Deals: traffic details
    hs_deals.utm_campaign,
    hs_deals.utm_content,
    hs_deals.utm_source,
    hs_deals.utm_term,
    hs_deals.utm_medium,
    hs_deals.utm_campaign_name,
    hs_deals.last_traffic_source,


    ---------- SOURCE: Auctions --------------

    -- Generic Auction Fields
    auc.has_winning_bid_any_auction,
    auc.number_of_auctions,
    auc.number_of_auction_cancellations,
    case when docs.is_sourced then 
        (case when auc.last_winning_bid_auction_type = 'RDA' then True else False end) end      as is_last_auction_rda_sourced,

    -- RDA: Auction Fields
    coalesce(rda.is_rda_sourced, false)                                                          as is_rda_sourced,
    coalesce(rfq.is_rfq_automatically_sourced or rda.is_rda_sourced, false)                      as has_winning_bid,
    rda.is_first_auction_rda_sourced,
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
    rda.number_of_planned_bids,

    --RDA: Winning Bid Fields
    rda.winning_bid_uuid,
    rda.winning_bid_margin,
    rda.winning_bid_margin_usd,
    rda.winning_bid_margin_loss_usd,
    rda.winning_shipping_estimate_amount_usd,
    rda.winning_l1_shipping_margin_amount_usd,
    rda.l1_shipping_estimate_source,
    rda.winning_bid_original_ship_by_date                                                        as original_shipping_at_by_supplier,
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
    docs.order_quote_is_admin,
    docs.order_quote_created_at,
    docs.order_quote_submitted_at,
    docs.order_quote_finalised_at,
    docs.order_quote_lead_time                                                                   as lead_time,
    docs.sourcing_window,
    docs.order_quote_price_multiplier                                                            as price_multiplier,
    geo.is_cross_docking_ind                                                                     as is_cross_docking,
    docs.order_quote_is_eligible_for_cross_docking                                               as is_eligible_for_cross_docking,
    docs.order_quote_is_local_sourcing                                                           as is_local_sourcing,
    docs.order_quote_is_eligible_for_local_sourcing                                              as is_eligible_for_local_sourcing,
    docs.order_quote_requires_local_production                                                   as requires_local_production,
    docs.rfq_quote_application,
    docs.rfq_quote_note,
    docs.rfq_quote_delivered_by,

    --Finance related exchange rates
    docs.order_quote_source_currency,
    docs.exchange_rate_at_closing,
    docs.exchange_rate_at_sourcing,

    --Documents: All Quotes
    docs.order_first_submitted_at,
    docs.number_of_quote_versions,
    docs.has_admin_created_quote,
    docs.has_manual_quote_review,
    docs.has_request_review,

    --Documents: First Purchase Order
    docs.po_first_uuid,
    docs.po_first_sourced_cost_usd,
    docs.sourced_at,
    docs.is_sourced,

    --Documents: Active Purchase Order
    docs.po_active_uuid,
    docs.po_active_finalized_at,
    docs.po_active_subtotal_cost_usd,
    docs.po_active_document_number,
    docs.po_active_company_entity,
    docs.po_active_support_ticket_id,
    case 
        when hubspot_technology_name = 'IM' then im_hs_promised_shipping_at_by_supplier
        else docs.po_active_promised_shipping_at_by_supplier end                                 as promised_shipping_at_by_supplier,
    case 
        when hubspot_technology_name = 'IM' then convert_timezone(origin_timezone, im_hs_promised_shipping_at_by_supplier)
        else convert_timezone(origin_timezone, po_active_promised_shipping_at_by_supplier) end   as localized_promised_shipping_at_by_supplier,

    --Documents: All Purchase Orders
    docs.number_of_purchase_orders,

    --Documents: Combined Fields
    docs.is_resourced,

    --------- SOURCE: STG ORDERS FINANCE ---------

    -- Finance: Stripe Fields
    finance.stripe_is_successful_payment,

    -- Finance: Netsuite Fields
    finance.payment_label,
    finance.order_remaining_amount                                                               as remaining_amount,
    finance.order_remaining_amount_usd                                                           as remaining_amount_usd,

    -- Finance: Fields from Combined Sources
    finance.is_auto_payment,
    finance.is_instant_payment,
    finance.payment_method,
    finance.is_pl_pay_later_used,
    finance.is_netsuite_batch_order,


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
    logistics.shipped_at                                                                         as order_shipped_at, -- Prefix to avoid ambiguous field
    convert_timezone(origin_timezone, logistics.shipped_at)                                      as localized_order_shipped_at,
    logistics.shipped_to_customer_at,
    logistics.shipped_from_cross_dock_at,
    logistics.shipment_label_created_at,

    -- Logistics: Delivery Dates
    logistics.delivered_at,
    logistics.full_delivered_at, -- Used for a definition
    logistics.derived_delivered_at,
    logistics.estimated_delivery_to_cross_dock_at,
    logistics.estimated_delivery_to_customer_at,
    logistics.delivered_to_cross_dock_at,
    docs.chargeable_shipping_weight_estimate_kg,

    -- Logsitics: Time spent at cross dock
    logistics_business_hours.time_transit_at_cross_dock_business_minutes,
    logistics_business_hours.time_cross_dock_rack_business_minutes,
    logistics_business_hours.lean_time_transit_at_cross_dock_business_minutes,

    -- Logistics: Estimates
    coalesce(case
        when rda.is_first_auction_rda_sourced then rda.first_winning_bid_estimated_first_leg_customs_amount_usd
        when not is_rfq_automatically_sourced then 
         case
        when
            po_production_finalized_at < coalesce(logistics.shipped_at, '2100-01-01') 
            then ppoli.estimated_l1_customs_amount_usd_no_winning_bid else  fpoli.estimated_l1_customs_amount_usd_no_winning_bid end
        else
            rfq.winning_bid_estimated_first_leg_customs_amount_usd
    end, 0)                                                                                      as estimated_l1_customs_amount_usd,

    coalesce(
        case
            when rda.is_first_auction_rda_sourced then rda.first_winning_bid_estimated_second_leg_customs_amount_usd else
                rfq.winning_bid_estimated_second_leg_customs_amount_usd
        end,
        0
    )                                                                                            as estimated_l2_customs_amount_usd,

    -------- SOURCE: STG OTR -----------
    -- Calculated based on cnc orders, and
    -- the stg tables of documents & logistics

    otr.is_shipped_on_time_by_supplier,
    otr.is_shipped_on_time_to_customer,
    otr.is_shipped_on_time_expected_by_customer,
    otr.shipping_to_customer_delay_days,
    otr.shipping_by_supplier_delay_days,
    -- Delay Notifications
    otr.has_delay_notifications,
    otr.number_of_delays,
    otr.has_delay_liability_supplier,
    otr.first_delay_created_at,
    otr.latest_new_shipping_at,
    -- Buffer Value
    otr.first_leg_buffer_value,
    -- Delay Probability
    otr.delay_probability,
    otr.delay_days_predicted,

    -------- SOURCE: AGG ORDERS LINE ITEMS --------

    -- Quote
    qli.number_of_part_line_items,
    qli.number_of_materials,
    qli.number_of_processes,
    qli.total_quantity,
    qli.total_weight_grams,
    qli.total_bounding_box_volume_cm3,
    qli.total_volume_cm3,
    qli.has_customer_note,
    qli.has_technical_drawings,
    qli.has_custom_material_subset,
    qli.has_custom_finish,
    qli.parts_amount_usd,
    qli.shipping_amount,
    qli.shipping_amount_usd,
    qli.discount_cost_usd,
    qli.other_line_items_amount_usd                                                              as other_amount_usd,
    qli.line_item_technology_id,
    qli.line_item_process_id                                                                     as process_id,
    qli.line_item_process_name                                                                   as process_name,
    qli.parts_titles,
    qli.parts_max_depth_cm,
    qli.parts_max_heigth_cm,
    qli.parts_max_width_cm,
    qli.price_amount_manually_edited_status,
    qli.price_amount_manually_edited_count,
    qli.quoting_package_versions,

    -- RND exclusive Fields
    qli.is_supply_or_smart_rfq,
    qli.total_smallest_bounding_box_volume_cm3,

    -- Purchase Orders
    fpoli.parts_amount_usd                                                                       as parts_cost_usd,
    fpoli.shipping_amount_usd                                                                    as po_first_shipping_cost_usd,
    fpoli.other_line_items_amount_usd                                                            as other_costs_usd,

    apoli.parts_amount_usd                                                                       as po_active_parts_cost_usd,
    apoli.shipping_amount_usd                                                                    as po_active_shipping_cost_usd,
    apoli.other_line_items_amount_usd                                                            as po_active_other_costs_usd,
    apoli.has_vqc_line_item                                                                      as is_vqced,
    apoli.has_coc_certification,

    ------ SOURCE: STG REVIEWS ---------
    -- Data from Technical Reviews

    reviews.has_technical_review,
    reviews.hubspot_first_technical_review_ongoing_at,
    reviews.hubspot_first_technical_review_completed_at,

    ------ SOURCE: STG GEO ------------
    -- Location data from customers,
    -- suppliers and company entity

    geo.destination_company_name,
    geo.destination_city,
    geo.destination_latitude,
    geo.destination_longitude,
    geo.destination_country_iso2,
    geo.destination_postal_code,
    geo.destination_country,
    geo.destination_market,
    geo.destination_region,
    geo.destination_sub_region,
    geo.destination_us_state,
    coalesce(geo.company_entity, po_active_company_entity) as company_entity, --Request by Bram S to fall back for nulls.
    geo.origin_country,
    geo.origin_latitude,
    geo.origin_longitude,
    geo.origin_market,
    geo.origin_region,

    ------ SOURCE: STG DEALSTAGE ---------
    -- Combines data from order history events (supply),
    -- hubspot dealstage history (hubspot).

    -- Closing
    coalesce(dealstage.is_closed, false)                                                          as is_closed,
    dealstage.closed_at,

    -- Cancellation
    dealstage.cancelled_at,

    -- Completion
    dealstage.first_completed_at, -- Used for a definition

    -- Status
    dealstage.order_status,

    -- Time spent in New
    dealstage.time_in_stage_new_business_minutes,

    -- Time spent in DFM for IM
    case
        when hubspot_technology_name = 'IM' then dealstage.im_deal_sourced_after_dfm_at
    end                                                                                          as im_deal_sourced_after_dfm_at,
    case
        when hubspot_technology_name = 'IM' then dealstage.time_in_stage_dfm_minutes
    end                                                                                          as time_in_stage_dfm_minutes,


    ------ SOURCE: STG INTERACTIONS ---------
    -- The stg table is derived from the aggregation of
    -- fact_interactions which combines the sources of
    -- freshdesk interactions and hubspot engagements.

    interactions.number_of_interactions,
    interactions.number_of_outgoing_emails,
    interactions.number_of_incoming_emails,
    interactions.number_of_interactions_fd,
    interactions.number_of_outgoing_emails_fd,
    interactions.number_of_incoming_emails_fd,
    interactions.number_of_notes_fd,

    ------ SOURCE: STG ORDER DISPUTES ---------
    -- Data from Disputes and Dispute Resolution

    -- Fields from Disputes Tables
    coalesce(disputes.is_quality_disputed, false)                                                as is_quality_disputed,
    disputes.dispute_created_at,
    disputes.dispute_requested_outcome,
    disputes.dispute_type,

    -- Fields from Dispute Resolutions
    disputes.dispute_resolution_at,
    disputes.dispute_resolution_time_hours,
    disputes.first_dispute_resolution_type,

    ---------- SOURCE: COMBINED FIELDS --------------
    -- Fields that are defined from two or more sources

    -- IDs
    coalesce(orders.hubspot_deal_id, hs_deals.hubspot_deal_id)                                   as order_hubspot_deal_id, -- Prefix to avoid ambiguous field
    coalesce(orders.number, docs.order_quote_document_number)                                    as document_number,

    -- Lifecycle:
    order_hubspot_deal_id is not null                                                            as exists_in_hubspot,
    order_quote_status
    = 'cart'                                                                                     as is_cart,
    case
        when order_quote_status = 'cart' then null else -- In June 2021 some carts started being created in HS
            coalesce(docs.order_first_submitted_at, hs_deals.hubspot_created_at)
    end                                                                                          as submitted_at,
    submitted_at is not null                                                                     as is_submitted,
    coalesce(nullif(hs_deals.hubspot_cancellation_reason, ''), pcr.cancellation_reason_title) as cancellation_reason,
    coalesce(
        nullif(hs_deals.hubspot_cancellation_reason_mapped, ''), pcr.cancellation_reason_mapped, cancellation_reason
    )                                                                                            as cancellation_reason_mapped,
    case
        when cancellation_reason_mapped in ('MP requested cancellation', 'Lead time cannot be met', 'Unusable files/Design cannot be manufactured')
            then 1
        else coalesce(auc.number_of_auction_cancellations, 0)
    end                                                                                          as number_of_cancellations,
    srl.is_recognized,
    srl.recognized_at,

    -- Technology:
    coalesce(rda.auction_technology_id, qli.line_item_technology_id, hubspot_technology_id)      as technology_id,
    coalesce(rda.technology_name, qli.line_item_technology_name,hubspot_technology_name)         as technology_name,

    -- Financial:
    coalesce(docs.order_quote_amount_usd, hs_deals.hubspot_amount_usd)                           as subtotal_amount_usd,
    --todo-migration-test datediff
    case when datediff('hours',docs.sourced_at, dealstage.cancelled_at) < 48 and   coalesce(
        nullif(hs_deals.hubspot_cancellation_reason_mapped, ''), pcr.cancellation_reason_mapped, cancellation_reason
    )  = 'New order created' then true else false end as exclude_cancelled_new_orders,
    case
        when is_closed and not exclude_cancelled_new_orders then subtotal_amount_usd else 0
    end                                                                                          as subtotal_closed_amount_usd,
    case
        when is_sourced and not exclude_cancelled_new_orders then subtotal_amount_usd else 0
    end                                                                                          as subtotal_sourced_amount_usd,
    case
        when is_logistics_shipping_quote_used = false and qli.line_item_technology_name = '3DP' then subtotal_amount_usd * 1.0 * 0.03
        when rda.is_first_auction_rda_sourced is not true and is_cross_docking = false then 0
        else qli.shipping_amount_usd
    end                                                                                          as prep_shipping_cost_usd,
    case
        when
            is_sourced and not exclude_cancelled_new_orders 
            then
                coalesce(case when rda.is_first_auction_rda_sourced then rda.first_winning_shipping_estimate_amount_usd end, 0)
                + coalesce(prep_shipping_cost_usd, 0)
        else 0
    end                                                                                          as shipping_cost_usd,
    case
        when
            po_production_finalized_at < coalesce(logistics.shipped_at, '2100-01-01') and coalesce(auc.has_winning_bid_any_auction, false) = false
            then po_production_subtotal_cost_usd
        else po_first_sourced_cost_usd
    end                                                                                          as subtotal_po_cost_usd,
    case
        when is_sourced and not exclude_cancelled_new_orders 
            then
                coalesce(subtotal_po_cost_usd, 0) + coalesce(shipping_cost_usd, 0)
                + estimated_l1_customs_amount_usd + estimated_l2_customs_amount_usd
        else 0
    end                                                                                          as subtotal_sourced_cost_usd,

    -- Suppliers:
    coalesce(docs.po_active_supplier_id, rda.supplier_id)                                        as supplier_id,



    -- Commission Related:
    case
        when
            hs_deals.hubspot_amount_usd - docs.order_quote_amount_usd - qli.shipping_amount_usd > 50 -- Threshold
            and is_closed is true and rfq.has_rfq = false then true
        when hs_deals.hubspot_amount_usd = 0 then true -- discussed with finance to have $0 amount deals not commissioned. 
        when is_closed is not true then null else false
    end                                                                                          as has_significant_amount_gap,
    coalesce(interactions.has_svp_interaction or qli.has_svp_line_item, false)                   as is_svp

from {{ ref('prep_supply_orders') }} as orders

    -- Staging
    left join {{ ref ('stg_orders_hubspot') }} as hs_deals on orders.hubspot_deal_id = hs_deals.hubspot_deal_id
    left join {{ ref ('stg_orders_documents') }} as docs on orders.uuid = docs.order_uuid
    left join {{ ref ('stg_orders_finance') }} as finance on orders.uuid = finance.order_uuid
    left join {{ ref ('stg_orders_logistics') }} as logistics on orders.uuid = logistics.order_uuid
    left join {{ ref ('stg_orders_logistics_business_hours') }} as logistics_business_hours on orders.uuid = logistics_business_hours.order_uuid
    left join {{ ref ('stg_orders_otr') }} as otr on orders.uuid = otr.order_uuid
    left join {{ ref ('stg_orders_geo') }} as geo on orders.uuid = geo.order_uuid
    left join {{ ref ('stg_orders_dealstage') }} as dealstage on orders.uuid = dealstage.order_uuid
    left join {{ ref ('stg_orders_disputes') }} as disputes on orders.uuid = disputes.order_uuid
    left join {{ ref ('stg_orders_users') }} as users on orders.uuid = users.order_uuid
    left join {{ ref ('stg_recognition_logic') }} as srl on orders.uuid = srl.order_uuid

    -- Reporting
    left join {{ ref ('fact_discounts') }} as discounts on orders.uuid = discounts.order_uuid

    -- Aggregates
    left join {{ ref ('agg_orders_rda') }} as rda on orders.uuid = rda.order_uuid
    left join {{ ref ('agg_orders_rfq') }} as rfq on orders.uuid = rfq.order_uuid
    left join {{ ref ('agg_orders_auctions') }} as auc on orders.uuid = auc.order_uuid
    left join {{ ref ('agg_orders_technical_reviews') }} as reviews on orders.uuid = reviews.order_uuid
    left join {{ ref ('agg_orders_interactions') }} as interactions on orders.hubspot_deal_id = interactions.hubspot_deal_id
    left join {{ ref ('agg_line_items') }} as qli on orders.quote_uuid = qli.quote_uuid -- Agg Order-Quotes
    left join {{ ref ('agg_line_items') }} as fpoli on docs.po_first_uuid = fpoli.quote_uuid -- Agg First POs
    left join {{ ref ('agg_line_items') }} as apoli on docs.po_active_uuid = apoli.quote_uuid -- Agg Active POs 
    left join {{ ref ('agg_line_items') }} as ppoli on docs.po_production_uuid = ppoli.quote_uuid  -- Agg Production POs     

    -- Data Lake
    left join {{ ref ('prep_supply_integration') }} as integration on orders.uuid = integration.order_uuid
    
    -- Service Supply
    left join {{ ref('prep_cancellation_reasons') }} as pcr on orders.cancellation_reason_id = pcr.cancellation_reason_id

where
    true
    and orders.legacy_order_id is null -- We take legacy orders from int_analytics.legacy_orders table as source of truth in a later stage
    and coalesce(orders.hubspot_deal_id, -9) != 1062498043 -- Manufacturing agreement, orders were logged separately
    and coalesce(orders.hubspot_deal_id, -9) != 9665453990 -- Revamp of a big order that landed in December 2020, to be filtered out as indicated by Marnix.
    and (coalesce(hs_deals.hubspot_contact_email_from_internal, false) = false or subtotal_sourced_amount_usd > 0)
