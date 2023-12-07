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
        from {{ ref('prep_bids') }} as bids
        inner join winning_bid_legacy on winning_bid_legacy.uuid = bids.uuid
    )
    
    select
    -- Primary Key
    auctions.uuid  as auction_uuid,
    -- Foreign Keys
    psd.order_uuid as order_uuid,
    psd.parent_uuid as quote_uuid,
    -- Auction Type
    decode(auctions.is_rfq, 'true', True, 'false', False) as is_rfq,
    case when is_rfq = 'true' then 'RFQ' else 'RDA' end as auction_type,
    -- Auctions Fields
    auctions.status,  -- If auction gets status 'resourced' it means it has been brought back to the auction
    auctions.started_at,
    auctions.finished_at,
    auctions.last_processed_at,
    auctions.base_margin,  -- For debugging purposes only, do not use for reporting
    auctions.base_margin_without_discount,
    decode(auctions.is_accepted_manually, 'true', True, 'false', False) as is_accepted_manually,
    decode(auctions.is_resourcing, 'true', True, 'false', False) as is_resourcing,
    decode(auctions.is_internal_support_ticket_opened, 'true', True, 'false', False) as is_internal_support_ticket_opened,
    auctions.internal_support_ticket_opened_at,     
    auctions.internal_support_ticket_id,
    -- Quotes Fields
    psd.created as auction_created_at,
    psd.updated as auction_updated_at,
    psd.deleted as auction_deleted_at,
    psd.document_number as auction_document_number,
    psd.technology_id,
    case when auction_type = 'RDA' then round((psd.subtotal_price_amount / 100.00), 2)
        when auction_type = 'RFQ' then null end as auction_amount_usd,
    -- Winning Bid
    coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) as winning_bid_uuid,
    -- Multiple Auctions Per Order
    row_number() over (partition by psd.order_uuid order by auctions.started_at desc nulls last) as recency_idx,
    row_number() over (partition by psd.order_uuid, case when coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) is not null then 1 else 0 end
        order by auctions.started_at asc nulls last) =1 and coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid, srl.prep_winning_bid_uuid) is not null as first_successful_auction,
    decode(recency_idx, 1, True, False) as is_latest_order_auction,
    row_number() over (partition by psd.order_uuid, auction_type  order by auctions.started_at desc nulls last) as recency_idx_auction_type,
    case when auction_type='RDA' then decode(recency_idx_auction_type, 1, True, False) end as is_latest_rda_order_auction,
    case when is_latest_rda_order_auction and is_latest_order_auction and winner_bid_uuid is not null then true else false end as is_rda_sourced,
     
    -- Technology Name
    technologies.name as technology_name

from {{ source('int_service_supply', 'auctions') }} as auctions
    inner join {{ ref('prep_supply_documents') }} as psd on auctions.uuid = psd.uuid
    left join supplier_rfq_winning_bid_legacy srl on srl.auction_uuid = auctions.uuid
    left join {{ ref ('technologies') }} as technologies on psd.technology_id = technologies.technology_id