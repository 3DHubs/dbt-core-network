/* The auctions table that is populated from Supply's `auction` table only holds true auctions. Currently the
   auctions mechanic is also used to track RFQs and the associated bids are used to get the price indication from the
   MP side. This table is enriched by left joining on the quote table of type auction.

   In Q1 a new feature was added to give discounts to customers. This resulted in a change of how margins are
   reflected on auctions. Auction-related tables (auctions, bids, supplier_auctions) should use the margin without
   discount -- the price offered to the MP is not affected by customer discounts.

   Another update in Q1 2021: a new feature was added to the RDA where parts can be resourced for whatever reason. We
   leverage the quotes table to get a sequence of auctions for a given order. If an order goes onto the RDA multiple
   times, it will yield a `recency_idx` >= 2. */
   
with auctions as (select
                      -- Primary Key
                      auctions.uuid  as auction_uuid,
                      -- Foreign Keys
                      oqs.order_uuid as order_uuid,
                      oqs.parent_uuid as quote_uuid,
                      -- Auction Attributes
                      auctions.winner_bid_uuid as winning_bid_uuid,
                      auctions.status,  -- If auction gets status 'resourced' it means it has been brought back to the auction
                      auctions.started_at,
                      auctions.finished_at,
                      auctions.base_margin,  -- For debugging purposes only, do not use for reporting
                      auctions.base_margin_without_discount,                    
                      decode(auctions.is_accepted_manually, 'true', True, 'false', False)                     as is_accepted_manually,
                      decode(auctions.is_resourcing, 'true', True, 'false', False)                            as is_resourcing,
                      decode(auctions.is_internal_support_ticket_opened, 'true', True, 'false', False)        as is_internal_support_ticket_opened,
                      auctions.internal_support_ticket_opened_at,     
                      -- Fields from Quotes Table (type Auction)
                      oqs.created as auction_created_at,
                      oqs.document_number,
                      oqs.technology_id,
                      round((oqs.subtotal_price_amount / 100.00), 2) as auction_amount_usd,
                      oqs.document_number as auction_document_number,
                      -- Fields from Suppliers
                      suppliers.id as auction_supplier_id,
                      suppliers.address_id as auction_supplier_address_id,
                      suppliers.name as auction_supplier_name,
                      -- Fields from Technologies
                      technologies.name as auction_technology_name,
                      -- One order can have multiple auctions
                      row_number() over (partition by oqs.order_uuid order by auctions.started_at desc nulls last)
                      as recency_idx
                  from {{ source('int_service_supply', 'auctions') }} as auctions
                      inner join {{ ref('prep_supply_documents') }} as oqs on auctions.uuid = oqs.uuid
                      left join {{ ref ('prep_bids')}} as bids on auctions.winner_bid_uuid = bids.uuid
                      left join {{ ref('suppliers') }} as suppliers on bids.supplier_id = suppliers.id
                      left join {{ ref ('technologies') }} as technologies on oqs.technology_id = technologies.technology_id
                  where not decode(auctions.is_rfq, 'true', True, 'false', False))
select *,
       decode(recency_idx, 1, True, False) as is_latest_order_auction
from auctions