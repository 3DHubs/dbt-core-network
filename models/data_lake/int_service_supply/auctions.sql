/* The auctions table that is populated from Supply's `auction` table only holds true auctions. Currently the
   auctions mechanic is also used to track RFQs and the associated bids are used to get the price indication from the
   MP side.

   In Q1 a new feature was added to give discounts to customers. This resulted in a change of how margins are
   reflected on auctions. Auction-related tables (auctions, bids, supplier_auctions) should use the margin without
   discount -- the price offered to the MP is not affected by customer discounts.

   Another update in Q1 2021: a new feature was added to the RDA where parts can be resourced for whatever reason. We
   leverage the quotes table to get a sequence of auctions for a given order. If an order goes onto the RDA multiple
   times, it will yield a `recency_idx` >= 2. */
with auctions as (select oqs.created,
                         oqs.updated,
                         oqs.deleted,
                         oqs.order_uuid                                                      as order_uuid,
                         auctions.uuid                                                       as order_quotes_uuid,
                         auctions.winner_bid_uuid,
                         auctions.status,                       -- If auction gets status 'resourced' it means it has been brought back to the auction
                         auctions.started_at,
                         auctions.finished_at,
                         auctions.ship_by_date,
                         auctions.last_processed_at,
                         auctions.internal_support_ticket_id,
                         auctions.internal_support_ticket_opened_at,
                         {{ varchar_to_boolean('is_internal_support_ticket_opened') }}, -- From `auctions`
                         decode(auctions.china_throttled, 'true', True, 'false', False)      as is_china_throttled,
                         auctions.base_margin,                  -- For debugging purposes only, do not use for reporting
                         auctions.base_margin_without_discount, -- This field will be used in auctions
                         auctions.next_allocate_at,
                         decode(auctions.is_accepted_manually, 'true', True, 'false', False) as is_accepted_manually,
                         decode(auctions.is_resourcing, 'true', True, 'false', False)        as is_resourcing,
                         row_number() over (partition by oqs.order_uuid order by auctions.started_at desc nulls last)
                                                                                             as recency_idx
                  from int_service_supply.auctions as auctions
                           inner join {{ ref('cnc_order_quotes') }} as oqs
                                      on auctions.uuid = oqs.uuid
                  where not decode(auctions.is_rfq, 'true', True, 'false', False))
select *,
       decode(recency_idx, 1, True, False) as is_latest_order_auction
from auctions