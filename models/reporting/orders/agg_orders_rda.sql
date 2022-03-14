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

with rda_interactions as ( 
    select sai.order_uuid,
           count(distinct auction_uuid)                                                              as number_of_rda_auctions,
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
           --Winning Bid Results
           bool_or(sai.is_winning_bid)                                                               as has_winning_bid,
           bool_or(sai.is_winning_bid and sai.response_type = 'accepted')                            as has_accepted_winning_bid,
           bool_or(sai.sa_is_restricted)                                                             as has_restricted_suppliers,
           bool_or(sai.is_winning_bid and sai.sa_is_restricted)                                      as has_restricted_winning_bid,
           max(case when sai.is_winning_bid then sai.bid_margin end)                                 as winning_bid_margin,
           max(case when sai.is_winning_bid then sai.bid_margin_usd end)                             as winning_bid_margin_usd,
           max(case when sai.is_winning_bid then sai.bid_margin_loss_usd end)                        as winning_bid_margin_loss_usd,
           bool_or(sai.bid_has_changed_prices and sai.is_winning_bid)                                as has_winning_bid_countered_on_price,
           bool_or(sai.bid_has_changed_shipping_date and sai.is_winning_bid)                         as has_winning_bid_countered_on_lead_time,
           bool_or(sai.bid_has_design_modifications and sai.is_winning_bid)                          as has_winning_bid_countered_on_design

    from {{ ref('fact_rda_behaviour') }} as sai
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
    select order_uuid
    from {{ source('int_service_supply', 'order_change_requests') }}
    where order_uuid in (select order_uuid from canceled)
    and status = 'rejected'
),
cancelled_auctions as (
    select distinct a.order_uuid,
        true as auction_is_cancelled_manually,
        c.min_created as auction_cancelled_manually_at
    from {{ ref('auctions_rda') }} a
            left join canceled c on c.order_uuid = a.order_uuid
    where a.status = 'canceled'
    and a.order_uuid not in (select order_uuid from rejected)   
), 

eligibility_sample as (
    select orders.uuid as order_uuid,
        count (*) as number_of_eligible_suppliers,
        count (case when is_preferred = 'true' then true end) as number_of_eligible_preferred_suppliers,
        count (case when is_local = 'true' then true end) as number_of_eligible_local_suppliers
    from {{ source('int_service_supply', 'matching_scores') }} as ms
    inner join {{ ref('cnc_orders') }} as orders on ms.quote_uuid = orders.quote_uuid
    group by 1
)

select
-------------------- Step 2 --------------------------
------------- Combine Data Sources  ------------------

case when auctions.finished_at is not null then true else false end as is_rda_sourced,

-- SOURCE 1: Adds fields from the rda interactions CTE
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
rdai.has_winning_bid,
rdai.winning_bid_margin,
rdai.winning_bid_margin_usd,
rdai.winning_bid_margin_loss_usd,
rdai.has_accepted_winning_bid,
rdai.has_restricted_suppliers,
rdai.has_restricted_winning_bid,
rdai.has_winning_bid_countered_on_price,
rdai.has_winning_bid_countered_on_lead_time,
rdai.has_winning_bid_countered_on_design,

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
auctions.document_number                   as auction_document_number,
auctions.is_accepted_manually              as auction_is_accepted_manually,
auctions.is_internal_support_ticket_opened as auction_is_reviewed_manually,
auctions.internal_support_ticket_opened_at as auction_support_ticket_opened_at,
auctions.winning_bid_uuid,
auctions.auction_supplier_id,
auctions.auction_supplier_name,
auctions.auction_supplier_address_id,
auctions.auction_technology_name,

-- SOURCE 4: Eligibility Sample
-- Product Feature that determines the number of suppliers that are eligible, preferred and local for a given quote.
es.number_of_eligible_suppliers,
es.number_of_eligible_preferred_suppliers,
es.number_of_eligible_local_suppliers

from {{ ref('auctions_rda') }} as auctions
    left join rda_interactions as rdai on auctions.order_uuid = rdai.order_uuid
    left join cancelled_auctions as can on auctions.order_uuid = can.order_uuid
    left join eligibility_sample as es on auctions.order_uuid = es.order_uuid
where auctions.is_latest_order_auction
