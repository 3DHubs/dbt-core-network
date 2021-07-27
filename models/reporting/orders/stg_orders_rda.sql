----------------------------------------------------------------
-- REVERSE DUTCH AUCTION (RDA) AGGREGATES
----------------------------------------------------------------

-- Sources:
-- 1. RDA Interactions Aggregates (a.k.a supplier auction interactions)
-- 2. Cancelled Auctions Query (Derived from supply_order_history_events and supply_order_change_requests)
-- 3. Data Lake Supply Auctions (coming from supply auctions table)
-- 4. Data Lake Bids (used for a join)
-- 5. Suppliers (used to bring supplier id)


-------------------- Step 1 --------------------------
---------- Create RDA Interactions Aggregates --------

-- SOURCE 1: RDA Interactions
-- In the table below data about the RDA is aggregated,
-- In the RDA multiple suppliers are assigned and these interact (or not) in the auction
-- A supplier can see (or not) the auction, bid at target, reject, counterbid...

-- Parent Sources: Auctions, Bids, Supplier Auctions, Order Quotes & Technologies

with rda_interactions as (
    select sai.auction_order_uuid as order_uuid,
           count(distinct sai.sa_supplier_id)                                                        as number_of_suppliers_assigned,
           --General Bid Aggregates
           count(distinct sai.bid_uuid)                                                              as number_of_bids,
           count(distinct (case when sai.bid_supplier_response = 'countered' then sai.bid_uuid end)) as
                                                                                                        number_of_counterbids,
           count(distinct (case when sai.bid_supplier_response = 'rejected' then sai.bid_uuid end))  as
                                                                                                        number_of_rejected_bids,
           --Counter Bids Count by Type
           count(distinct (case when sai.bid_has_design_modifications then sai.bid_uuid end))        as
                                                                                                        number_of_design_counterbids,
           --todo:replace data type at source
           count(distinct (case
                               when sai.bid_has_changed_shipping_date = 'true'
                                   then sai.bid_uuid end))                                           as number_of_lead_time_counterbids,
           count(distinct
                 (case when sai.bid_has_changed_prices then sai.bid_uuid end))                       as number_of_price_counterbids,
           --Winning Bid Results
           bool_or(sai.is_winning_bid)                                                               as order_has_winning_bid,
           bool_or(sai.is_winning_bid and sai.bid_supplier_response = 'accepted')                    as order_has_accepted_winning_bid,
           bool_or(sai.bid_has_changed_prices and sai.is_winning_bid)                                as order_has_winning_bid_countered_on_price,
           bool_or(
                   sai.bid_has_changed_shipping_date = 'true' and sai.is_winning_bid)                as order_has_winning_bid_countered_on_lead_time,
           bool_or(sai.bid_has_design_modifications and sai.is_winning_bid)                          as order_has_winning_bid_countered_on_design
           --todo: DBT table does not contain the order_uuid
    from {{ ref('fact_supplier_auction_interactions') }} as sai
    group by 1

    -- SOURCE 2: Auctions Cancelled Manually
),
canceled as (
    select order_uuid, min(created) as min_created
    from {{ ref('order_history_events') }}
    where description like 'Canceled an auction'
    group by 1
),
rejected as (
    select order_uuid
    from {{ ref('order_change_requests') }}
    where order_uuid in (select order_uuid from canceled)
    and status = 'rejected'
),
cancelled_auctions as (
    select distinct a.order_uuid,
        true as auction_is_cancelled_manually,
        c.min_created as auction_cancelled_manually_at
    from {{ ref('auctions') }} a
            left join canceled c on c.order_uuid = a.order_uuid
    where a.status = 'canceled'
    and a.order_uuid not in (select order_uuid from rejected)   
)

select
-------------------- Step 2 --------------------------
------------- Combine Data Sources  ------------------

true as is_rda_sourced,

-- SOURCE 1: Adds fields from the rda interactions CTE
rdai.number_of_suppliers_assigned,
rdai.number_of_bids,
rdai.number_of_counterbids,
rdai.number_of_rejected_bids,
rdai.number_of_design_counterbids,
rdai.number_of_lead_time_counterbids,
rdai.number_of_price_counterbids,
rdai.order_has_winning_bid,
rdai.order_has_accepted_winning_bid,
rdai.order_has_winning_bid_countered_on_price,
rdai.order_has_winning_bid_countered_on_design,

-- SOURCE 2: Auctions Manually Cancelled
can.auction_is_cancelled_manually,
can.auction_cancelled_manually_at,

-- SOURCE 3: Auctions
-- Data comes from the auctions table and it is enriched with data from
-- the quotes table (with type = auction).
auctions.order_uuid,
auctions.order_quotes_uuid                 as auction_uuid,
auctions.status                            as auction_status,
auctions.created                           as auction_created_at,
auctions.finished_at                       as auction_finished_at,
auctions.is_accepted_manually              as auction_is_accepted_manually,
auctions.is_internal_support_ticket_opened as auction_is_reviewed_manually,
auctions.internal_support_ticket_opened_at as auction_support_ticket_created_at,
auctions.winner_bid_uuid                   as winning_bid_uuid,

-- SOURCE 3B: Enriches Auctions with data from quotes table
a_quotes.technology_id                     as auction_technology_id,
a_quotes.document_number                   as auction_document_number,

-- SOURCE 3C: Enriches Auctions with supplier data

suppliers.id as auction_supplier_id,
suppliers.name as auction_supplier_name,
suppliers.address_id as auction_supplier_address_id,

-- SURCE 3D: Enriches Auctions with technology data
technologies.name as auction_technology_name

    -- TODO: if agreed, remove fields below
    -- started_at,
    -- last_processed_at, 

from {{ ref('auctions') }} as auctions
    inner join {{ ref('cnc_order_quotes') }} as a_quotes on auctions.order_quotes_uuid = a_quotes.uuid
    left join {{ ref('bids') }} as bids on auctions.winner_bid_uuid = bids.uuid
    left join {{ ref('suppliers') }} as suppliers on bids.supplier_id = suppliers.id
    left join rda_interactions as rdai on a_quotes.order_uuid = rdai.order_uuid
    left join cancelled_auctions as can on a_quotes.order_uuid = can.order_uuid
    left join {{ ref ('technologies') }} as technologies on a_quotes.technology_id = technologies.technology_id

where auctions.is_latest_order_auction
