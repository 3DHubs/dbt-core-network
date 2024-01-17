select
    bids.created,
    bids.updated,
    bids.deleted,
    bids.uuid,
    bids.auction_uuid,
    bids.response_type,
    bids.placed_at,
    bids.loss_reason as bid_loss_reason,
    bids.explanation,
    bids.ship_by_date,
    bids.supplier_id,
    bids.accepted_ship_by_date,
    bids.author_id,
    bids.margin,  -- For debugging purposes only, do not use for reporting
    bids.margin_without_discount,  -- This field will be used in auctions,
    bids.revision as bid_version,  -- Used to calculate active vs 1st bid version.
    bids.subtotal_price_amount,
    bids.currency_code,
    bids.description,
    bids.lead_time,
    br.title,
    round(
        (old_bids.estimated_first_leg_customs_amount_usd / 100.00), 2
    ) as estimated_first_leg_customs_amount_usd,
    round(
        (old_bids.estimated_second_leg_customs_amount_usd / 100.00), 2
    ) as estimated_second_leg_customs_amount_usd,
    case
        when bids.has_changed_prices = 'true'
        then true
        when bids.has_changed_prices = 'false'
        then false
    end as has_changed_prices,
    case
        when bids.has_design_modifications = 'true'
        then true
        when bids.has_design_modifications = 'false'
        then false
    end as has_design_modifications,
    case
        when bids.has_changed_shipping_date = 'true'
        then true
        when bids.has_changed_shipping_date = 'false'
        then false
    end as has_changed_shipping_date,
    case
        when bids.is_active = 'true'
        then true
        when bids.is_active = 'false'
        then false
    end as is_active
from {{ source("int_service_supply", "new_bids") }} as bids
left join
    {{ source("int_service_supply", "bids") }} as old_bids on bids.uuid = old_bids.uuid
left join 
    {{ ref("bids_bid_reasons") }} as bbr on bids.uuid = bbr.bid_uuid
left join
    {{ source("int_service_supply", "bid_reasons") }} as br on bbr.bid_reasons_id = br.id