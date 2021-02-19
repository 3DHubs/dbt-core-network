select oqs.created,
       oqs.updated,
       oqs.deleted,
       auctions.uuid as order_quotes_uuid,
       auctions.winner_bid_uuid,
       auctions.status,
       auctions.started_at,
       auctions.finished_at,
       auctions.ship_by_date,
       auctions.last_processed_at,
       auctions.internal_support_ticket_id,
       decode(auctions.is_internal_support_ticket_opened, 'true', True, 'false', False) as is_internal_support_ticket_opened,
       decode(auctions.china_throttled, 'true', True, 'false', False) as is_china_throttled,
       auctions.base_margin,
       auctions.next_allocate_at,
       decode(auctions.is_rfq, 'true', True, 'false', False) as is_rfq
from int_service_supply.auctions as auctions
         inner join {{ ref('cnc_order_quotes') }} as oqs
                    on auctions.uuid = oqs.uuid