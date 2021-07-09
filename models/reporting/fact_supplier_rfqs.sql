with winning_bid as (
        select oqsl.parent_uuid as uuid

        from {{ ref('cnc_order_quotes') }} oqsl
            inner join  {{ ref('purchase_orders') }} spocl
                on oqsl.uuid = spocl.uuid

        where oqsl.type = 'purchase_order'
            and spocl.status = 'active'

        group by 1
     ),
     -- for each order it is only possible to have 1 RFQ sent per supplier
     -- however it is possible for an admin to duplicate a quote that has been RFQed, which creates a new RFQ auction (as well as bids)
     -- example order_uuid = 00189119-d4e6-4c79-bed9-7969956f09bf
     -- we only want to consider the first RFQ bid response for each supplier so create bid_index here
     supplier_rfq_bids as (
        select bid_quotes.*,
               bids.placed_at,
               bids.uuid                                                                              as bid_uuid,
               bids.supplier_id,
               row_number()
               over (partition by bid_quotes.order_uuid, supplier_id order by bid_quotes.created asc) as supplier_bid_idx
        
        from {{ ref('cnc_order_quotes') }} as bid_quotes
            left outer join {{ ref('bids') }} as bids
                on bid_quotes.uuid = bids.uuid
            left outer join {{ ref('cnc_order_quotes') }} as auction_quote
                on auction_quote.uuid = bid_quotes.parent_uuid
            inner join {{ ref('supplier_rfqs_src_auction') }} as auctions
                on auctions.order_quotes_uuid = auction_quote.uuid
        
        where bid_quotes.type = 'bid'
     )

select supplier_rfqs.id                                                                     as supplier_rfq_id,
       supplier_rfqs.order_uuid,
       supplier_rfqs.supplier_id,
       s.name                                                                               as supplier_name,
       supplier_rfqs.created                                                                as supplier_rfq_sent_date,
       bid_quotes.lead_time,
       round(bid_quotes.subtotal_price_amount / 100.00, 2)                                  as rfq_bid_amount,
       bid_quotes.currency_code                                                             as rfq_bid_amount_currency,
       round(((bid_quotes.subtotal_price_amount / 100.00) / rates.rate), 2)::decimal(15, 2) as rfq_bid_amount_usd,
       case when winning_bid.uuid > 0 then true else false end                              as is_winning_bid,
       bid_quotes.placed_at                                                                 as supplier_rfq_responded_date

from {{ ref('supplier_rfqs') }} as supplier_rfqs
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