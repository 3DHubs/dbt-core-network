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
    freshdesk_rfq_value as (
        select order_uuid,
        requester_email_domain,
        value as quality_value_score
        from {{ ref("fact_freshdesk_tickets") }}
        where
            "group" in ('Injection Molding EU/RoW', 'Injection Molding US/CA')
            and value is not null
    ),
    -- applicable to orders before July 2023
     winning_bid_legacy as ( 
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
            bids.response_type,
            case when bids.response_type = 'rejected' then null else bids.subtotal_price_amount end as subtotal_price_amount,
            bids.lead_time,
            bids.placed_at,
            bids.uuid as bid_uuid,
            case when bids.uuid = coalesce(auctions.winner_bid_uuid,winning_bid_legacy.uuid) then true else false end as is_winning_bid_prep,
            bids.supplier_id,
            bids.accepted_ship_by_date,
            bids.ship_by_date,
            bids.estimated_first_leg_customs_amount_usd as bid_estimated_first_leg_customs_amount_usd,
            bids.estimated_second_leg_customs_amount_usd as bid_estimated_second_leg_customs_amount_usd,
            md5(bids.supplier_id || bids.auction_uuid) as supplier_rfq_uuid,
            row_number() over (
                partition by bids.uuid, supplier_id order by bids.created asc
            ) as supplier_bid_idx
        from {{ ref("prep_bids") }} as bids
        inner join  -- Inner Join to Filter on RFQ
            {{ ref("prep_auctions_rfq") }} as auctions
            on auctions.order_quotes_uuid = bids.auction_uuid
        left join winning_bid_legacy
            on winning_bid_legacy.uuid = bids.uuid
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
    )

    -- Combines Supplier-Auctions + Bid Data + Others

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
            sds.supplier_name as supplier_name,
            -- Data from Bids
            bid_quotes.lead_time,
            round(bid_quotes.subtotal_price_amount / 100.00, 2) as rfq_bid_amount,
            bid_quotes.currency_code as rfq_bid_amount_currency,
            round(
                ((bid_quotes.subtotal_price_amount / 100.00) / rates.rate), 2
            )::decimal(15, 2) as rfq_bid_amount_usd,
            coalesce(bid_quotes.is_winning_bid_prep,false) as is_winning_bid,
            bid_quotes.placed_at as supplier_rfq_responded_date,
            bid_quotes.response_type,
            bid_quotes.accepted_ship_by_date,
            bid_quotes.ship_by_date,
            bid_quotes.bid_estimated_first_leg_customs_amount_usd,
            bid_quotes.bid_estimated_second_leg_customs_amount_usd,
            rank() over (
                        partition by rfq_a.order_uuid, rfq_a.supplier_id
                        -- prepares logic to get per order the unique responses of a
                        -- supplier
                        order by
                            is_winning_bid desc, response_type,
                            coalesce(bid_quotes.placed_at, '2000-01-01') desc
                    ) as win_rate_rank,
            case
                when win_rate_rank = 1 and is_winning_bid
                then 1
                -- the winning bid counts positive and 1 other bid per order per
                -- supplier counts negative to the win rate
                when win_rate_rank = 1 and response_type = 'countered'
                then 0
                else null
            end as supplier_win_rate,
            -- Data Source
            'Supplier-Auctions' as data_source,
            -- RFQ quality score for IM deals
            frv.requester_email_domain,
            quality_value_score,
            first_value(is_winning_bid) over (partition by rfq_a.auction_uuid order by is_winning_bid desc rows between unbounded preceding and unbounded following)        as has_winning_bid_on_auction,
            first_value(bid_quotes.lead_time) over (partition by rfq_a.auction_uuid order by is_winning_bid desc rows between unbounded preceding and unbounded following)        as winning_bid_lead_time,
            first_value(rfq_bid_amount_usd) over (partition by rfq_a.auction_uuid order by is_winning_bid desc rows between unbounded preceding and unbounded following)        as winning_bid_amount_usd,
            case when has_winning_bid_on_auction then rfq_bid_amount_usd*1.0/nullif(winning_bid_amount_usd,0) end as bid_amount_percent_of_winning_bid,
            case when has_winning_bid_on_auction then lead_time*1.0/nullif(winning_bid_lead_time,0) end as leadtime_percent_of_winning_bid,
            case when has_winning_bid_on_auction then count(case when response_type='countered' then auction_uuid end) over (partition by auction_uuid) end  as number_of_counter_bids_in_auction
           
        from supplier_rfq_auctions as rfq_a
        left outer join
            supplier_rfq_bids as bid_quotes
            on rfq_a.supplier_rfq_uuid = bid_quotes.supplier_rfq_uuid
        left outer join {{ ref("stg_dim_suppliers") }} as sds on sds.supplier_id = rfq_a.supplier_id
        left outer join
            {{ ref("exchange_rate_daily") }} as rates
            on rates.currency_code_to = bid_quotes.currency_code
            and trunc(bid_quotes.created) = trunc(rates.date)
        left outer join freshdesk_rfq_value frv on frv.order_uuid =  rfq_a.order_uuid and frv.requester_email_domain = sds.supplier_email_domain
    


