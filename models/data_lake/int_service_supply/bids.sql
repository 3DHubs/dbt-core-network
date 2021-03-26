select oqs.created,
       oqs.updated,
       oqs.deleted,
       bids.uuid,
       bids.auction_uuid,
       bids.response_type,
       bids.placed_at,
       decode(bids.has_changed_prices, 'true', True, 'false', False)       as has_changed_prices,
       decode(bids.has_design_modifications, 'true', True, 'false', False) as has_design_modifications,
       bids.ship_by_date,
       bids.rejection_text,
       decode(bids.is_active, 'true', True, 'false', False)                as is_active,
       bids.supplier_id,
       bids.has_changed_shipping_date,
       bids.rejection_reasons,
       bids.accepted_ship_by_date,
       bids.author_id,
       bids.margin, -- For debugging purposes only, do not use for reporting
       bids.margin_without_discount -- This field will be used in auctions
from int_service_supply.bids as bids
         inner join {{ ref('cnc_order_quotes') }} as oqs
                    on bids.uuid = oqs.uuid