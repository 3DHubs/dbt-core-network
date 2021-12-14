select oqs.created,
       oqs.updated,
       oqs.deleted,
       bids.uuid,
       bids.auction_uuid,
       bids.response_type,
       bids.placed_at,
       {{ varchar_to_boolean('has_changed_prices') }}, -- From `bids`
       {{ varchar_to_boolean('has_design_modifications') }}, -- From `bids`
       {{ varchar_to_boolean('has_changed_shipping_date') }}, -- From `bids`
       bids.ship_by_date,
       {{ varchar_to_boolean('is_active') }}, -- From `bids`
       bids.supplier_id,
       bids.accepted_ship_by_date,
       bids.author_id,
       bids.margin, -- For debugging purposes only, do not use for reporting
       bids.margin_without_discount -- This field will be used in auctions
from {{ source('int_service_supply', 'bids') }} as bids
         inner join {{ ref('cnc_order_quotes') }} as oqs
                    on bids.uuid = oqs.uuid