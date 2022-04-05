select oqs.created,
       oqs.updated,
       oqs.deleted,
       bids.uuid,
       bids.auction_uuid,
       bids.response_type,
       bids.placed_at,
       bids.bid_loss_reason,
       bids.explanation,
       bids.ship_by_date,
       bids.supplier_id,
       bids.accepted_ship_by_date,
       bids.author_id,
       bids.margin, -- For debugging purposes only, do not use for reporting
       bids.margin_without_discount, -- This field will be used in auctions,
       {{ varchar_to_boolean('has_changed_prices') }},
       {{ varchar_to_boolean('has_design_modifications') }},
       {{ varchar_to_boolean('has_changed_shipping_date') }},
       {{ varchar_to_boolean('is_active') }}
from {{ source('int_service_supply', 'bids') }} as bids
         inner join {{ ref('supply_documents') }} as oqs
                    on bids.uuid = oqs.uuid
