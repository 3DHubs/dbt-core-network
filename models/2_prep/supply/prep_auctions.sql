with
     winning_bid_legacy as (
        select psd.parent_uuid as uuid
        from {{ ref('prep_supply_documents') }} psd
        inner join {{ ref('prep_purchase_orders') }} ppo on psd.uuid = ppo.uuid
        where psd.type = 'purchase_order' and ppo.status = 'active'
        group by 1
    ),

    supplier_rfq_winning_bid_legacy as (
        select
            bids.auction_uuid,
            bids.uuid as prep_winning_bid_uuid
        from {{ ref('bids') }} as bids
        inner join winning_bid_legacy on winning_bid_legacy.uuid = bids.uuid
    )
    
    select
    -- Primary Key
    auctions.auction_uuid,
    -- Foreign Keys
    auctions.order_uuid,
    auctions.quote_uuid,
    -- Auction Type
    is_rfq,
    auction_type,
    -- Auctions Fields
    auctions.status,  -- If auction gets status 'resourced' it means it has been brought back to the auction
    auctions.started_at,
    auctions.finished_at,
    auctions.last_processed_at,
    auctions.expected_sourcing_window_end_at,
    auctions.base_margin,  -- For debugging purposes only, do not use for reporting
    auctions.base_margin_without_discount,
    auctions.is_accepted_manually,
    auctions.is_resourcing,
    auctions.is_internal_support_ticket_opened,
    auctions.internal_support_ticket_opened_at,     
    auctions.internal_support_ticket_id,
    -- Quotes Fields
    auctions.auction_created_at,
    auctions.auction_updated_at,
    auctions.auction_deleted_at,
    auctions.auction_document_number,
    auctions.technology_id,
    case when auction_type = 'RDA' then auctions.auction_subtotal_price_amount
        when auction_type = 'RFQ' then null end as auction_amount_usd,
    -- Order Level Fields
    ali.li_subtotal_amount_usd,
    ali.discount_cost_usd,
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd) as auction_quote_amount_usd,
    -- Winning Bid
    srl.prep_winning_bid_uuid as srl_prep_winning_bid_uuid,
    coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) as winning_bid_uuid,
    -- Multiple Auctions Per Order
    row_number() over (partition by auctions.order_uuid order by auctions.started_at desc nulls last) as recency_idx,
    row_number() over (partition by auctions.order_uuid, case when coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) is not null then 1 else 0 end
        order by auctions.started_at asc nulls last) =1 and coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) is not null as first_successful_auction,
    row_number() over (partition by auctions.order_uuid, case when coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) is not null then 1 else 0 end
    order by auctions.finished_at desc nulls last) =1 and coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) is not null as last_successful_auction,
    row_number() over (partition by auctions.order_uuid, auction_type  order by auctions.started_at desc nulls last) as recency_idx_auction_type,
    case when auction_type='RDA' then decode(recency_idx_auction_type, 1, True, False) end as is_latest_rda_order_auction,
    case when is_latest_rda_order_auction and last_successful_auction and winner_bid_uuid is not null then true else false end as is_rda_sourced,
     
    -- Technology Name
    auctions.technology_name

from {{ ref('auctions') }} as auctions
    inner join {{ ref('prep_supply_documents') }} as psd on auctions.auction_uuid = psd.uuid and psd.deleted is null
    left join supplier_rfq_winning_bid_legacy srl on srl.auction_uuid = auctions.auction_uuid
    left join {{ ref ('technologies') }} as technologies on psd.technology_id = technologies.technology_id
    left join {{ ref("agg_line_items") }} as ali on psd.parent_uuid = ali.quote_uuid
