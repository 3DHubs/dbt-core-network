----------------------------------------------------------------
-- REVERSE DUTCH AUCTION (RDA) AGGREGATES
----------------------------------------------------------------

-- Sources:
-- 1. RDA Interactions Aggregates (a.k.a supplier auction interactions)
-- 2. Cancelled Auctions Query (Derived from supply_order_history_events and supply_order_change_requests)
-- 3. Data Lake Auctions RDA (filtered from supply auctions table)
-- 4. Eligibility Sample Data (a.k.a matching score)

-------------------- Step 1 --------------------------
---------- Create RDA Interactions Aggregates --------

-- SOURCE 1: RDA Interactions
-- In the table below data about the RDA is aggregated,
-- In the RDA multiple suppliers are assigned and these interact (or not) in the auction
-- A supplier can see (or not) the auction, bid at target, reject, counterbid...

-- Parent Sources: Auctions, Bids, Supplier Auctions, Order Quotes & Technologies

{{ config(
    tags=["multirefresh"]
) }}

with rda_interactions as ( 
    select sai.order_uuid,
           bool_or(case when pa.auction_type = 'RDA' then true else false end)                       as is_first_auction_rda_sourced,
           bool_or(pa.is_rda_sourced)                                                                as is_rda_sourced,
           count(distinct sai.auction_uuid)                                                          as number_of_rda_auctions,
           count(*)                                                                                  as number_of_supplier_auctions_assigned,
           -- Auctions Seen
           count(distinct (case when sa_first_seen_at is not null then sa_uuid end))                 as number_of_supplier_auctions_seen,
           --General Bid Aggregates 
           count(distinct sai.bid_uuid)                                                              as number_of_responses,
           count(distinct (case when sai.response_type in ('accepted','countered')  
                                then sai.bid_uuid end))                                              as number_of_positive_responses,
           count(distinct (case when sai.response_type = 'countered' then sai.bid_uuid end))         as number_of_countered_responses,
           count(distinct (case when sai.response_type = 'rejected' then sai.bid_uuid end))          as number_of_rejected_responses,
           --Counter Bids Count by Type
           count(distinct (case when sai.bid_has_design_modifications then sai.bid_uuid end))        as number_of_design_counterbids,
           count(distinct (case
                               when sai.bid_has_changed_shipping_date = 'true' -- todo:replace data type at source
                                   then sai.bid_uuid end))                                           as number_of_lead_time_counterbids,
           count(distinct
                 (case when sai.bid_has_changed_prices then sai.bid_uuid end))                       as number_of_price_counterbids,
           count(distinct
                 (case when sai.plan_to_bid_at is not null then sai.bid_uuid end))                   as number_of_planned_bids,
           --Winning Bid Results
           bool_or(sai.is_winning_bid)                                                               as has_winning_bid,
           bool_or(sai.is_winning_bid and sai.response_type = 'accepted')                            as has_accepted_winning_bid,
           bool_or(sai.sa_is_restricted)                                                             as has_restricted_suppliers,
           bool_or(sai.is_winning_bid and sai.sa_is_restricted)                                      as has_restricted_winning_bid,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sa_supplier_id end) as supplier_id,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.bid_margin end)                                 as winning_bid_margin,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.bid_margin_usd end)                             as winning_bid_margin_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.bid_margin_loss_usd end)                        as winning_bid_margin_loss_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.shipping_estimate_amount_usd end)               as winning_shipping_estimate_amount_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.l1_shipping_margin_amount_usd end)              as winning_l1_shipping_margin_amount_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.dhl_shipping_price_estimate_amount_usd end)     as winning_dhl_shipping_price_estimate_amount_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.bid_estimated_first_leg_customs_amount_usd end)     as winning_bid_estimated_first_leg_customs_amount_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.bid_estimated_second_leg_customs_amount_usd end)     as winning_bid_estimated_second_leg_customs_amount_usd,
           max(case when sai.is_winning_bid and sai.last_auction_winning_bid = 1 then sai.original_ship_by_date end)                      as winning_bid_original_ship_by_date,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.bid_margin end)                                 as first_winning_bid_margin,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.bid_margin_usd end)                             as first_winning_bid_margin_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.bid_margin_loss_usd end)                        as first_winning_bid_margin_loss_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.shipping_estimate_amount_usd end)               as first_winning_shipping_estimate_amount_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.l1_shipping_margin_amount_usd end)              as first_winning_l1_shipping_margin_amount_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.dhl_shipping_price_estimate_amount_usd end)     as first_winning_dhl_shipping_price_estimate_amount_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.bid_estimated_first_leg_customs_amount_usd end)     as first_winning_bid_estimated_first_leg_customs_amount_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.bid_estimated_second_leg_customs_amount_usd end)     as first_winning_bid_estimated_second_leg_customs_amount_usd,
           max(case when sai.is_winning_bid and sai.first_auction_winning_bid = 1 then sai.original_ship_by_date end)                      as first_winning_bid_original_ship_by_date,
           bool_or(sai.bid_has_changed_prices and sai.is_winning_bid)                                as has_winning_bid_countered_on_price,
           bool_or(sai.bid_has_changed_shipping_date and sai.is_winning_bid)                         as has_winning_bid_countered_on_lead_time,
           bool_or(sai.bid_has_design_modifications and sai.is_winning_bid)                          as has_winning_bid_countered_on_design

    from {{ ref('fact_auction_behaviour') }} as sai
    left join {{ ref('prep_auctions')}} as pa on sai.auction_uuid = pa.auction_uuid and pa.first_successful_auction
    where not sai.is_rfq
    group by 1

    -- SOURCE 2: Auctions Cancelled Manually
),
canceled as (
    select order_uuid, min(created) as min_created
    from {{ ref('fact_order_events') }}
    where std_event_id = 222
    group by 1
),
rejected as (
    select uuid
    from {{ ref('prep_supply_orders') }}
    where uuid in (select order_uuid from canceled)
    and order_change_request_status = 'rejected'
),
cancelled_auctions as (
    select distinct a.order_uuid,
        true as auction_is_cancelled_manually,
        c.min_created as auction_cancelled_manually_at
    from {{ ref('prep_auctions') }} a
            left join canceled c on c.order_uuid = a.order_uuid 
    where a.status = 'canceled' and not a.is_rfq
    and a.order_uuid not in (select uuid from rejected)   
), 

eligibility_sample as (
    select orders.uuid as order_uuid,
        count (*) as number_of_eligible_suppliers,
        count (case when is_preferred = 'true' then true end) as number_of_eligible_preferred_suppliers,
        count (case when is_local = 'true' then true end) as number_of_eligible_local_suppliers
    from {{ ref('sources_network', 'gold_matching_scores') }} as ms
    inner join {{ ref('prep_supply_orders') }} as orders on ms.quote_uuid = orders.quote_uuid
    group by 1
)

select
-------------------- Step 2 --------------------------
------------- Combine Data Sources  ------------------

rdai.is_first_auction_rda_sourced, 
rdai.is_rda_sourced,
-- SOURCE 1: Adds fields from the rda interactions CTE
rdai.supplier_id,
rdai.number_of_rda_auctions,
rdai.number_of_supplier_auctions_assigned,
rdai.number_of_supplier_auctions_seen,
rdai.number_of_responses,
rdai.number_of_positive_responses,
rdai.number_of_countered_responses,
rdai.number_of_rejected_responses,
rdai.number_of_design_counterbids,
rdai.number_of_lead_time_counterbids,
rdai.number_of_price_counterbids,
rdai.number_of_planned_bids,
rdai.has_winning_bid,
rdai.first_winning_bid_margin,
rdai.first_winning_bid_margin_usd,
rdai.first_winning_bid_margin_loss_usd,
rdai.first_winning_shipping_estimate_amount_usd,
rdai.first_winning_l1_shipping_margin_amount_usd,
rdai.first_winning_dhl_shipping_price_estimate_amount_usd,
rdai.first_winning_bid_estimated_first_leg_customs_amount_usd,
rdai.first_winning_bid_estimated_second_leg_customs_amount_usd,
rdai.first_winning_bid_original_ship_by_date,
rdai.winning_bid_margin,
rdai.winning_bid_margin_usd,
rdai.winning_bid_margin_loss_usd,
rdai.has_accepted_winning_bid,
rdai.has_restricted_suppliers,
rdai.has_restricted_winning_bid,
rdai.has_winning_bid_countered_on_price,
rdai.has_winning_bid_countered_on_lead_time,
rdai.has_winning_bid_countered_on_design,
rdai.winning_l1_shipping_margin_amount_usd,
rdai.winning_shipping_estimate_amount_usd,
rdai.winning_bid_estimated_first_leg_customs_amount_usd,
rdai.winning_bid_estimated_second_leg_customs_amount_usd,
rdai.winning_bid_original_ship_by_date,
case when rdai.winning_shipping_estimate_amount_usd = rdai.winning_dhl_shipping_price_estimate_amount_usd then 'api' else 'backup' end as l1_shipping_estimate_source,
-- SOURCE 2: Auctions Manually Cancelled
can.auction_is_cancelled_manually,
can.auction_cancelled_manually_at,

-- SOURCE 3: Auctions
-- Data comes from the auctions table and it is enriched with data from
-- the quotes table (with type = auction).
auctions.order_uuid,
auctions.auction_uuid,
auctions.status                            as auction_status,
auctions.auction_created_at,
auctions.started_at                        as auction_started_at,
auctions.finished_at                       as auction_finished_at,
auctions.technology_id                     as auction_technology_id,
auctions.auction_document_number,
auctions.is_accepted_manually              as auction_is_accepted_manually,
auctions.is_internal_support_ticket_opened as auction_is_reviewed_manually,
auctions.internal_support_ticket_opened_at as auction_support_ticket_opened_at,
auctions.winning_bid_uuid,
auctions.technology_name,

-- SOURCE 4: Eligibility Sample
-- Product Feature that determines the number of suppliers that are eligible, preferred and local for a given quote.
es.number_of_eligible_suppliers,
es.number_of_eligible_preferred_suppliers,
es.number_of_eligible_local_suppliers

from {{ ref('prep_auctions') }} as auctions
    left join rda_interactions as rdai on auctions.order_uuid = rdai.order_uuid
    left join cancelled_auctions as can on auctions.order_uuid = can.order_uuid
    left join eligibility_sample as es on auctions.order_uuid = es.order_uuid
where auctions.is_latest_rda_order_auction