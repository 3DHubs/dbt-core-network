/* 
This table contains data  at the supplier-rfq auctions-interaction level.
This means that an order can have many RFQs as well as suppliers assigned.
The table is built from two sources: auctions (RFQ) and a legacy supplier_rfqs table in int service supply.
The legacy table contains only one-to-one relationship whereas auctions reflect the new one-to-many process.
 */

with winning_bid as (
    select oqsl.parent_uuid as uuid

    from {{ ref('cnc_order_quotes') }} oqsl
             inner join {{ ref('purchase_orders') }} spocl
                        on oqsl.uuid = spocl.uuid

    where oqsl.type = 'purchase_order'
      and spocl.status = 'active'

    group by 1
),

    -- Query all supplier bids that were RFQs

    supplier_rfq_bids as (
        select bid_quotes.*,
            bids.placed_at,
            bids.uuid                                                                              as bid_uuid,
            bids.supplier_id,
            md5(bids.supplier_id || bids.auction_uuid)                                             as supplier_rfq_uuid,
            row_number()
            over (partition by bid_quotes.order_uuid, supplier_id order by bid_quotes.created asc) as supplier_bid_idx
        from  {{ ref('cnc_order_quotes') }} as bid_quotes
                left join {{ ref('bids') }} as bids
                                on bid_quotes.uuid = bids.uuid
                left join {{ ref('cnc_order_quotes') }} as auction_quote
                                on auction_quote.uuid = bid_quotes.parent_uuid
                inner join {{ ref('auctions_rfq') }} as auctions -- Inner Join to Filter on RFQ
                            on auctions.order_quotes_uuid = auction_quote.uuid
        where bid_quotes.type = 'bid'
    ),
    
    -- Query all supplier auctions that were RFQ'ed. 
    -- One order can have multiple auctions per suppliers

    supplier_rfq_auctions as (
        select md5(sa.supplier_id || sa.auction_uuid) as supplier_rfq_uuid,
            assigned_at                            as rfq_sent_date,
            supplier_id,
            order_uuid,
            auction_uuid,
            sr.auction_document_number
        from {{ source('int_service_supply', 'supplier_auctions') }} as sa
                        left join {{ source('int_service_supply', 'auctions') }} as auctions
                                on sa.auction_uuid = auctions.uuid
                inner join {{ ref('auctions_rfq') }} sr on sr.order_quotes_uuid = sa.auction_uuid -- Inner Join to Filter on RFQ
                where true
    ),
    
    -- Query all supplier auctions that were RFQ'ed, as well as the response from suppliers
    supplier_rfq_auction_interactions as (
        select supplier_rfq_auctions.supplier_rfq_uuid                                           as supplier_rfq_uuid,
            supplier_rfq_auctions.order_uuid,
            supplier_rfq_auctions.supplier_id,
            supplier_rfq_auctions.auction_document_number,
            s.name                                                                               as supplier_name,
            supplier_rfq_auctions.rfq_sent_date                                                  as supplier_rfq_sent_date,
            bid_quotes.lead_time,
            round(bid_quotes.subtotal_price_amount / 100.00, 2)                                  as rfq_bid_amount,
            bid_quotes.currency_code                                                             as rfq_bid_amount_currency,
            round(((bid_quotes.subtotal_price_amount / 100.00) / rates.rate), 2)::decimal(15, 2) as rfq_bid_amount_usd,
            case when winning_bid.uuid > 0 then true else false end                              as is_winning_bid,
            bid_quotes.placed_at                                                                 as supplier_rfq_responded_date,
            'auctions'                                                                           as data_source

        from supplier_rfq_auctions
                left outer join supplier_rfq_bids as bid_quotes
                                on supplier_rfq_auctions.supplier_rfq_uuid = bid_quotes.supplier_rfq_uuid
                left outer join {{ ref('suppliers') }} as s
                                on s.id = supplier_rfq_auctions.supplier_id
                left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                                on rates.currency_code_to = bid_quotes.currency_code
                                    and trunc(bid_quotes.created) = trunc(rates.date)
                left outer join winning_bid
                                on bid_quotes.bid_uuid = winning_bid.uuid
    ),
    
    -- Query old supplier RFQs - these are orders that were RFQ'ed before we automated the RFQ process (and also started creating an auction for supplier RFQs)
    -- Supplier_rfqs only allows 1 RFQ request per supplier (per order) when in reality an order can have mulitple RFQ requests from the same order
    -- so this is the supplier RFQ table is only used to collect historical data.
    
    supplier_rfqs as (
        select supplier_rfqs.id::varchar                                                         as supplier_rfq_uuid,
            supplier_rfqs.order_uuid,
            supplier_rfqs.supplier_id,
            null as auction_document_number,
            s.name                                                                               as supplier_name,
            supplier_rfqs.created                                                                as supplier_rfq_sent_date,
            bid_quotes.lead_time,
            round(bid_quotes.subtotal_price_amount / 100.00, 2)                                  as rfq_bid_amount,
            bid_quotes.currency_code                                                             as rfq_bid_amount_currency,
            round(((bid_quotes.subtotal_price_amount / 100.00) / rates.rate), 2)::decimal(15, 2) as rfq_bid_amount_usd,
            case when winning_bid.uuid > 0 then true else false end                              as is_winning_bid,
            bid_quotes.placed_at                                                                 as supplier_rfq_responded_date,
            'supplier_rfqs'                                                                      as data_source

        from  {{ source('int_service_supply', 'supplier_rfqs') }} as supplier_rfqs
                left outer join supplier_rfq_bids as bid_quotes
                                on supplier_rfqs.order_uuid = bid_quotes.order_uuid
                                    and bid_quotes.supplier_id = supplier_rfqs.supplier_id
                                    and supplier_bid_idx = 1
                left outer join {{ ref('suppliers') }} as s
                                on s.id = supplier_rfqs.supplier_id
                left outer join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                                on rates.currency_code_to = bid_quotes.currency_code
                                    and trunc(bid_quotes.created) = trunc(rates.date)
                left outer join winning_bid
                                on bid_quotes.bid_uuid = winning_bid.uuid
    )

-- Union rfq supplier auctions and supplier rfqs to get an overview of all RFQs sent 
select *
from supplier_rfq_auction_interactions

union all

select *
from supplier_rfqs
where order_uuid not in (select order_uuid from supplier_rfq_auction_interactions)