/*
This table contains data  at the supplier-rfq auctions-interaction level.
This means that an order can have many RFQs as well as suppliers assigned.
The table is built from two sources: auctions (RFQ) and a legacy supplier_rfqs table in int service supply.
The legacy table contains only one-to-one relationship whereas auctions reflect the new one-to-many process.
 */
-- Data Sources
-- 1. Supplier-Auctions: combined with auctions (rfq type) and bids (rfq type) data.
-- 2. Supplier-RFQ: legacy data when there was only one rfq per order.
{{ config(tags=["multirefresh"]) }}

with
    stg_supplier_auctions as (

        select md5(supplier_id || auction_uuid) as supplier_rfq_uuid, *
        from {{ ref("supplier_auctions") }}

    ),
    winning_bid as (
        select oqsl.parent_uuid as uuid
        from {{ ref("prep_supply_documents") }} oqsl
        inner join {{ ref("prep_purchase_orders") }} spocl on oqsl.uuid = spocl.uuid
        where oqsl.type = 'purchase_order' and spocl.status = 'active'
        group by 1

    ),
    -- Data from Bids (RFQ)
    supplier_rfq_bids as (
        select
            bids.created,
            bids.currency_code,
            bids.subtotal_price_amount,
            bids.lead_time,
            bids.placed_at,
            bids.uuid as bid_uuid,
            bids.supplier_id,
            bids.accepted_ship_by_date,
            bids.ship_by_date,
            md5(bids.supplier_id || bids.auction_uuid) as supplier_rfq_uuid,
            row_number() over (
                partition by bids.uuid, supplier_id
                order by bids.created asc
            ) as supplier_bid_idx
        from {{ ref("prep_bids") }} as bids
        inner join  -- Inner Join to Filter on RFQ
            {{ ref("prep_auctions_rfq") }} as auctions
            on auctions.order_quotes_uuid = bids.auction_uuid
    ),

    -- Data from Supplier-Auctions (RFQ) + Auctions (RFQ)
    supplier_rfq_auctions as (
        select
            sa.supplier_rfq_uuid,
            sa.assigned_at as rfq_sent_date,
            sa.supplier_id,
            sa.auction_uuid,
            sa.is_automatic_rfq as is_automatically_allocated_rfq,
            sa.original_ship_by_date,
            sr.order_uuid,
            sr.auction_document_number
        from stg_supplier_auctions as sa
        inner join  -- Inner Join to Filter on RFQ
            {{ ref("prep_auctions_rfq") }} sr on sr.order_quotes_uuid = sa.auction_uuid
    ),

    -- Combines Supplier-Auctions + Bid Data + Others
    supplier_rfq_auction_interactions as (
        select
            -- Data from Supplier-Auctions (RFQ)
            rfq_a.supplier_rfq_uuid as supplier_rfq_uuid,
            rfq_a.order_uuid,
            rfq_a.auction_uuid,
            rfq_a.auction_document_number,
            rfq_a.supplier_id,
            rfq_a.is_automatically_allocated_rfq,
            rfq_a.rfq_sent_date as supplier_rfq_sent_date,
            rfq_a.original_ship_by_date,
            -- Data from Suppliers
            s.name as supplier_name,
            -- Data from Bids
            bid_quotes.lead_time,
            round(bid_quotes.subtotal_price_amount / 100.00, 2) as rfq_bid_amount,
            bid_quotes.currency_code as rfq_bid_amount_currency,
            round(
                ((bid_quotes.subtotal_price_amount / 100.00) / rates.rate), 2
            )::decimal(15, 2) as rfq_bid_amount_usd,
            case when winning_bid.uuid > 0 then true else false end as is_winning_bid,
            bid_quotes.placed_at as supplier_rfq_responded_date,
            bid_quotes.accepted_ship_by_date,
            bid_quotes.ship_by_date,
            case
                when bid_quotes.placed_at is not null
                then
                    rank() over (
                        partition by rfq_a.order_uuid, rfq_a.supplier_id
                        -- prepares logic to get per order the unique responses of a
                        -- supplier
                        order by
                            is_winning_bid desc,
                            coalesce(bid_quotes.placed_at, '2000-01-01') desc
                    )
            end as win_rate_rank,
            case
                when win_rate_rank = 1 and is_winning_bid
                then 1
                -- the winning bid counts positive and 1 other bid per order per
                -- supplier counts negative to the win rate
                when win_rate_rank = 1
                then 0
                else null
            end as supplier_win_rate,
            -- Data Source
            'Supplier-Auctions' as data_source

        from supplier_rfq_auctions as rfq_a
        left outer join
            supplier_rfq_bids as bid_quotes
            on rfq_a.supplier_rfq_uuid = bid_quotes.supplier_rfq_uuid
        left outer join {{ ref("suppliers") }} as s on s.id = rfq_a.supplier_id
        left outer join
            {{ source("data_lake", "exchange_rate_spot_daily") }} as rates
            on rates.currency_code_to = bid_quotes.currency_code
            and trunc(bid_quotes.created) = trunc(rates.date)
        left outer join winning_bid on bid_quotes.bid_uuid = winning_bid.uuid
    ),

    -- -------- SOURCE: 2. SUPPLIER RFQs ----------------------
    -- Query old supplier RFQs - these are orders that were RFQ'ed before we automated
    -- the RFQ process (and also started creating an auction for supplier RFQs)
    -- Supplier_rfqs only allows 1 RFQ request per supplier (per order) when in
    -- reality an order can have mulitple RFQ requests from the same order
    -- so this is the supplier RFQ table is only used to collect historical data.
    supplier_rfqs as (
        select
            supplier_rfqs.id::varchar as supplier_rfq_uuid,
            supplier_rfqs.order_uuid,
            null as auction_uuid,
            null as auction_document_number,
            supplier_rfqs.supplier_id,
            null as is_automatically_allocated_rfq,  -- Feature only exists in new data
            supplier_rfqs.created as supplier_rfq_sent_date,
            null as original_ship_by_date,
            s.name as supplier_name,
            bid_quotes.lead_time,
            round(bid_quotes.subtotal_price_amount / 100.00, 2) as rfq_bid_amount,
            bid_quotes.currency_code as rfq_bid_amount_currency,
            round(
                ((bid_quotes.subtotal_price_amount / 100.00) / rates.rate), 2
            )::decimal(15, 2) as rfq_bid_amount_usd,
            case when winning_bid.uuid > 0 then true else false end as is_winning_bid,
            bid_quotes.placed_at as supplier_rfq_responded_date,
            bid_quotes.accepted_ship_by_date,
            bid_quotes.ship_by_date,

            case
                when bid_quotes.placed_at is not null
                then
                    rank() over (
                        partition by supplier_rfqs.order_uuid, supplier_rfqs.supplier_id
                        order by
                            is_winning_bid desc,
                            coalesce(bid_quotes.placed_at, '2000-01-01') desc
                    )
            end as win_rate_rank,
            case
                when win_rate_rank = 1 and is_winning_bid
                then 1
                when win_rate_rank = 1
                then 0
                else null
            end as supplier_win_rate,
            'supplier_rfqs' as data_source

        from {{ source("int_service_supply", "supplier_rfqs") }} as supplier_rfqs
        left outer join
            supplier_rfq_bids as bid_quotes
            on supplier_rfqs.order_uuid = bid_quotes.bid_uuid
            and bid_quotes.supplier_id = supplier_rfqs.supplier_id
            and supplier_bid_idx = 1
        left outer join {{ ref("suppliers") }} as s on s.id = supplier_rfqs.supplier_id
        left outer join
            {{ source("data_lake", "exchange_rate_spot_daily") }} as rates
            on rates.currency_code_to = bid_quotes.currency_code
            and trunc(bid_quotes.created) = trunc(rates.date)
        left outer join winning_bid on bid_quotes.bid_uuid = winning_bid.uuid
    )

-- -------- MAIN QUERY --------------
-- Union rfq supplier auctions and supplier rfqs to get an overview of all RFQs sent
select *
from supplier_rfq_auction_interactions

union all

select *
from supplier_rfqs
where order_uuid not in (select order_uuid from supplier_rfq_auction_interactions)
