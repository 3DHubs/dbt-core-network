-- Table Created by: Diego
-- Table Maintained by: Daniel Salazar Soplapuco
-- Last Edited: Dec 2022
-- Description: the base of this table is the supplier-auctions table which contains
-- one row for each
-- combination of supplier-auction, an auction has typically several suppliers
-- assigned. This table is
-- then enriched using the auctions table for auction fields (e.g. auction doc number)
-- and the bids table
-- where responses from the suppliers are stored.
{{ config(tags=["multirefresh"]) }}

with
    stg_supplier_auctions as (
        select md5(supplier_id || auction_uuid) as sa_uuid, *

        from {{ ref("supplier_auctions") }}
    ),

    bids as (
        select
            md5(b.supplier_id || b.auction_uuid) as sa_uuid,
            b.has_changed_prices,
            b.has_changed_shipping_date,
            b.has_design_modifications,
            b.created,
            b.updated,
            b.deleted,
            b.uuid,
            b.auction_uuid,
            case when b.placed_at is not null then b.response_type end as response_type,
            b.placed_at,
            b.is_active,
            b.supplier_id,
            b.ship_by_date,
            b.accepted_ship_by_date,
            b.description as design_modification_text,
            case
                when response_type = 'rejected'
                then null
                else round(((b.subtotal_price_amount / 100.00) / e.rate), 2)
            end as bid_amount_usd,
            case
                when response_type = 'rejected' then null else b.margin_without_discount
            end as margin_without_discount,
            b.estimated_first_leg_customs_amount_usd,
            b.estimated_second_leg_customs_amount_usd,
            b.title,
            b.explanation,
            b.bid_loss_reason,
            b.bid_version,
            row_number() over (
                partition by b.auction_uuid, b.supplier_id
                order by b.is_active desc, b.placed_at desc, b.updated desc
            ) as rn
        from {{ ref("prep_bids") }} as b
        left join
            {{ ref('exchange_rate_daily') }} as e
            on e.currency_code_to = b.currency_code
            and trunc(e.date) = trunc(b.created)
    )

select
    -- Supplier Auction Fields
    sa.sa_uuid as sa_uuid,
    sa.supplier_id as sa_supplier_id,
    suppliers.address_id as supplier_address_id,
    suppliers.name as supplier_name,
    sa.assigned_at as sa_assigned_at,
    row_number() over (
        partition by sa.auction_uuid order by sa_assigned_at
    ) as sa_auction_rank,
    sa.first_seen_at as sa_first_seen_at,
    sa.last_seen_at as sa_last_seen_at,
    sa.is_preferred_auction as sa_is_preferred_auction,
    -- Product Feature (a.k.a sourcing limbo)
    sa.is_restricted_auction as sa_is_restricted,
    coalesce(
        -- Primarily at Supplier Auction Level
        round((sa.subtotal_price_amount_usd / 100.00), 2), a.auction_amount_usd
    ) as sa_amount_usd,
    sa.margin_without_discount as sa_margin,
   round((sa.shipping_estimate_amount_usd / 100.00), 2) as shipping_estimate_amount_usd,
   round((sa.l1_shipping_margin_amount_usd / 100.00), 2) as l1_shipping_margin_amount_usd,
   round((sa.dhl_shipping_price_estimate_amount_usd / 100.00), 2) as dhl_shipping_price_estimate_amount_usd,


    -- Bid Fields
    b.uuid as bid_uuid,
    b.response_type as response_type,
    b.placed_at as response_placed_at,
    b.title as response_reason,  -- Reason for counterbid and rejections
    b.explanation as response_explanation,
    b.bid_loss_reason as bid_loss_reason,
    b.has_changed_prices as bid_has_changed_prices,
    b.has_design_modifications as bid_has_design_modifications,
    b.has_changed_shipping_date as bid_has_changed_shipping_date,
    b.bid_amount_usd as bid_amount_usd,
    b_1.bid_amount_usd as bid_original_amount_usd,
    b.margin_without_discount as bid_margin,
    b_1.margin_without_discount as bid_original_margin,
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd)
    * b.margin_without_discount as bid_margin_usd,
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd) * (
        a.base_margin_without_discount - b.margin_without_discount
    ) as bid_margin_loss_usd,
    case
        when b_1.response_type = 'countered' then true else false
    end as has_multiple_supplier_counter_bids_on_price,
    b.design_modification_text as design_modification_text,
    case when b.uuid = a.winning_bid_uuid then true else false end as is_winning_bid,
    b.estimated_first_leg_customs_amount_usd as bid_estimated_first_leg_customs_amount_usd,
    b.estimated_second_leg_customs_amount_usd as bid_estimated_second_leg_customs_amount_usd,

    -- Auction Fields
    sa.auction_uuid as auction_uuid,
    a.auction_created_at,
    a.order_uuid as order_uuid,
    a.winning_bid_uuid as auction_winning_bid_uuid,
    a.status as auction_status,
    a.finished_at as auction_finished_at,
    a.auction_document_number,
    a.base_margin_without_discount as auction_base_margin,
    a.recency_idx as auction_round,

    -- Order Level Fields
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd) as auction_quote_amount_usd,

    -- sourcing time fields:
    -- original ship by date.
    sa.original_ship_by_date,
    b.accepted_ship_by_date,  -- ship by date accepted by supplier.
    b.ship_by_date,     -- accepted ship by date + if counterbid + sourcing time and non business day adjustments.
    row_number() over (
                partition by order_uuid
                order by is_winning_bid desc, finished_at asc
            ) as first_auction_winning_bid,
    row_number() over (
            partition by order_uuid
            order by is_winning_bid desc, finished_at desc
        ) as last_auction_winning_bid
from stg_supplier_auctions as sa
-- Filter for only RDA Auctions
inner join {{ ref("prep_auctions") }} as a on a.auction_uuid = sa.auction_uuid and not a.is_rfq
left join bids as b on b.sa_uuid = sa.sa_uuid and b.rn = 1
left join {{ ref('suppliers') }} as suppliers on b.supplier_id = suppliers.id
-- To get 1st counter bid on price of supplier that is followed by a newer bid (to
-- identify negotiation winnings)
left join
    bids as b_1
    on b_1.sa_uuid = sa.sa_uuid
    and b_1.is_active = false
    and b_1.bid_version = 1
    and b_1.has_changed_prices
    and b_1.bid_amount_usd <> b.bid_amount_usd
    and b.bid_amount_usd > 0
left join {{ ref("agg_line_items") }} as ali on a.quote_uuid = ali.quote_uuid
