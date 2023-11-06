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
    coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid) as winning_bid_uuid,
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
    -- Winning Bid Supplier Info
    suppliers.id as auction_supplier_id,
    suppliers.address_id as auction_supplier_address_id,
    suppliers.name as auction_supplier_name,
    

from {{ source('int_service_supply', 'auctions') }} as auctions
    inner join {{ ref('prep_supply_documents') }} as psd on auctions.uuid = psd.uuid
    left join {{ ref ('prep_bids')}} as bids on coalesce(auctions.new_winner_bid_uuid, auctions.winner_bid_uuid) = bids.uuid
    left join {{ ref('suppliers') }} as suppliers on bids.supplier_id = suppliers.id