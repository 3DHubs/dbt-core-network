-- Table Create By: Xiaohan Li 202402
-- Refactor legacy models fact_rda_behaviour & fact_rfq_behaviour
-- This table contains bid level data of all auctions regardless of auction type.
-- One order could have multiple auctions of different auction types.

{{ config(tags=["multirefresh"]) }}

with bids as (
    select 
        md5(b.supplier_id || b.auction_uuid) as sa_uuid,
        b.has_changed_prices,
        b.has_changed_shipping_date,
        b.has_design_modifications,
        b.created,
        b.updated,
        b.uuid as bid_uuid,
        b.auction_uuid,
        b.placed_at,
        case when a.is_rfq then b.response_type
            else (case when b.placed_at is not null then b.response_type end) end as response_type,
        b.lead_time,
        b.is_active,
        b.supplier_id,
        b.ship_by_date,
        b.accepted_ship_by_date,
        b.description as design_modification_text,
        case when b.response_type = 'rejected' then null else b.subtotal_price_amount end as subtotal_price_amount,
        nullif(round((b.subtotal_price_amount / 100.00), 2),0) as bid_amount,
        nullif(round(((b.subtotal_price_amount / 100.00) / e.rate), 2),0) as bid_amount_usd,
        case when response_type = 'rejected' then null else b.margin_without_discount end as margin_without_discount,
        b.estimated_first_leg_customs_amount_usd,
        b.estimated_second_leg_customs_amount_usd,
        b.title,
        b.explanation,
        b.bid_loss_reason,
        b.bid_version,
        case when b.uuid = coalesce(a.winning_bid_uuid, a.srl_prep_winning_bid_uuid) then true else false end as is_winning_bid_prep,
        row_number() over (partition by b.auction_uuid, b.supplier_id order by b.is_active desc, b.placed_at desc, b.updated desc) as supplier_bid_idx
    from {{ ref('bids') }} as b
    inner join {{ ref("prep_auctions") }} as a on a.auction_uuid = b.auction_uuid
    left join {{ ref('exchange_rate_daily') }} as e on e.currency_code_to = b.currency_code and trunc(e.date) = trunc(b.created)
)

select
    -- Supplier Auction Fields
    sa.uuid as sa_uuid,
    sa.supplier_id as sa_supplier_id,
    sa.assigned_at as sa_assigned_at,
    row_number() over (partition by sa.auction_uuid order by sa_assigned_at) as sa_auction_rank,
    sa.first_seen_at as sa_first_seen_at,
    sa.last_seen_at as sa_last_seen_at,
    sa.is_preferred_auction as sa_is_preferred_auction,
    sa.is_restricted_auction as sa_is_restricted,
    coalesce(sa.subtotal_price_amount_usd, a.auction_amount_usd) as sa_amount_usd,
    sa.margin_without_discount as sa_margin,
    sa.shipping_estimate_amount_usd as shipping_estimate_amount_usd,
    sa.l1_shipping_margin_amount_usd as l1_shipping_margin_amount_usd,
    sa.dhl_shipping_price_estimate_amount_usd as dhl_shipping_price_estimate_amount_usd,
    sa.is_automatic_rfq as is_automatically_allocated_rfq,
    sa.ship_by_date as original_ship_by_date,

    -- Bid Fields
    b.bid_uuid,
    b.response_type,
    b.placed_at as response_placed_at,
    b.title as response_reason,  -- Reason for counterbid and rejections
    b.explanation as response_explanation,
    b.bid_loss_reason,
    b.has_changed_prices as bid_has_changed_prices,
    b.has_design_modifications as bid_has_design_modifications,
    b.has_changed_shipping_date as bid_has_changed_shipping_date,
    b.bid_amount,
    b.bid_amount_usd,
    b.margin_without_discount as bid_margin,
    (a.li_subtotal_amount_usd - a.discount_cost_usd) * b.margin_without_discount as bid_margin_usd,
    (a.li_subtotal_amount_usd - a.discount_cost_usd) * (a.base_margin_without_discount - b.margin_without_discount) as bid_margin_loss_usd,
    b.design_modification_text,
    case when a.is_rfq then coalesce(b.is_winning_bid_prep,false)
        else (case when bid_uuid = a.winning_bid_uuid then true else false end) end as is_winning_bid,
    b.estimated_first_leg_customs_amount_usd as bid_estimated_first_leg_customs_amount_usd,
    b.estimated_second_leg_customs_amount_usd as bid_estimated_second_leg_customs_amount_usd,
    b.lead_time,
    b.accepted_ship_by_date, -- ship by date accepted by supplier
    b.ship_by_date, -- accepted ship by date + if counterbid + sourcing time and non business day adjustments
    b.is_active as bid_is_active,

    -- Auction + Order Fields
    sa.auction_uuid,
    a.order_uuid,
    a.is_rfq,
    a.auction_type,
    a.auction_created_at,
    a.finished_at as auction_finished_at,
    a.expected_sourcing_window_end_at,
    a.winning_bid_uuid as auction_winning_bid_uuid,
    a.status as auction_status,
    a.auction_document_number,
    a.auction_quote_amount_usd,
    a.base_margin_without_discount as auction_base_margin,
    a.recency_idx as auction_round,
    
    -- Winning Bid Fields
    row_number() over (partition by order_uuid order by is_winning_bid desc, finished_at asc) as first_auction_winning_bid,
    row_number() over (partition by order_uuid order by is_winning_bid desc, finished_at desc) as last_auction_winning_bid,
    rank() over (partition by a.order_uuid, sa.supplier_id   -- the unique bids of a supplier per order
        order by is_winning_bid desc, response_type, coalesce(b.placed_at, '2000-01-01') desc) as win_rate_rank,
    case when win_rate_rank = 1 and is_winning_bid then 1 
        when win_rate_rank = 1 and response_type = 'countered' then 0
        else null end as supplier_win_rate,    -- the winning bid counts positive and 1 other bid per order per supplier counts negative to the win rate
    first_value(is_winning_bid) over (partition by sa.auction_uuid order by is_winning_bid desc rows between unbounded preceding and unbounded following) as has_winning_bid_on_auction,
    first_value(b.lead_time) over (partition by sa.auction_uuid order by is_winning_bid desc rows between unbounded preceding and unbounded following) as winning_bid_lead_time,
    first_value(bid_amount_usd) over (partition by sa.auction_uuid order by is_winning_bid desc rows between unbounded preceding and unbounded following) as winning_bid_amount_usd,
    case when has_winning_bid_on_auction then bid_amount_usd*1.0/nullif(winning_bid_amount_usd,0) end as bid_amount_percent_of_winning_bid,
    case when has_winning_bid_on_auction then lead_time*1.0/nullif(winning_bid_lead_time,0) end as leadtime_percent_of_winning_bid,
    case when has_winning_bid_on_auction then count(case when response_type='countered' then sa.auction_uuid end) over (partition by sa.auction_uuid) end as number_of_counter_bids_in_auction

from {{ ref("supplier_auctions") }} as sa
inner join {{ ref("prep_auctions") }} as a on a.auction_uuid = sa.auction_uuid
left join bids as b on b.sa_uuid = sa.uuid and b.supplier_bid_idx = 1
