-- Table Maintained by: Diego
-- Last Edited: Jan 2022

-- Description: the base of this table is the supplier-auctions table which contains one row for each
-- combination of supplier-auction, an auction has typically several suppliers assigned. This table is
-- then enriched using the auctions table for auction fields (e.g. auction doc number) and the bids table
-- where responses from the suppliers are stored.

with stg_supplier_auctions as (
    select md5(supplier_id || auction_uuid) as sa_uuid, *

    from {{ ref('supplier_auctions') }}
),

     bids as (
         select md5(b.supplier_id || b.auction_uuid)                                                        as sa_uuid,
                b.has_changed_prices,
                b.has_changed_shipping_date,
                b.has_design_modifications,
                b.created,
                b.updated,
                b.deleted,
                b.uuid,
                b.auction_uuid,
                case when b.placed_at is not null then b.response_type end                                  as response_type,
                b.placed_at,
                b.ship_by_date,
                b.is_active,
                b.supplier_id,
                b.accepted_ship_by_date,
                q.description                                                                               as design_modification_text,
                case
                    when response_type = 'rejected' then null
                    else round(((q.subtotal_price_amount / 100.00) / e.rate), 2) end                        as bid_amount_usd,
                case
                    when response_type = 'rejected' then null
                    else b.margin_without_discount end                                                      as margin_without_discount,
                br.title,
                b.explanation,
                b.bid_loss_reason,
                row_number() over (partition by b.auction_uuid, b.supplier_id order by b.updated desc, b.placed_at desc) as rn
         from {{ ref('bids') }} as b 
            left join {{ ref('cnc_order_quotes') }} as q on b.uuid = q.uuid
            left join {{ ref('bids_bid_reasons') }} as bbr on b.uuid = bbr.bid_uuid
            left join {{ source('int_service_supply', 'bid_reasons') }} as br on bbr.bid_reasons_id = br.id
            left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as e on e.currency_code_to = q.currency_code and trunc(e.date) = trunc(q.created)
     )

select
    -- Supplier Auction Fields
    sa.sa_uuid                                                                        as sa_uuid,
    sa.supplier_id                                                                    as sa_supplier_id,
    sa.assigned_at                                                                    as sa_assigned_at,
    row_number()
    over (partition by sa.auction_uuid order by sa_assigned_at)                       as sa_auction_rank,
    sa.first_seen_at                                                                  as sa_first_seen_at,
    sa.last_seen_at                                                                   as sa_last_seen_at,
    sa.is_preferred_auction                                                           as sa_is_preferred_auction,
    sa.is_restricted_auction                                                          as sa_is_restricted,         -- Product Feature (a.k.a sourcing limbo)
    coalesce(round((sa.subtotal_price_amount_usd / 100.00), 2),
             a.auction_amount_usd)                                                    as sa_amount_usd,            -- Primarily at Supplier Auction Level
    sa.margin_without_discount                                                        as sa_margin,

    -- Bid Fields
    b.uuid                                                                            as bid_uuid,
    b.response_type                                                                   as response_type,
    b.placed_at                                                                       as response_placed_at,
    b.title                                                                           as response_reason, -- Reason for counterbid and rejections
    b.explanation                                                                     as response_explanation,
    b.bid_loss_reason                                                                 as bid_loss_reason,
    b.has_changed_prices                                                              as bid_has_changed_prices,
    b.has_design_modifications                                                        as bid_has_design_modifications,
    b.has_changed_shipping_date                                                       as bid_has_changed_shipping_date,
    b.bid_amount_usd                                                                  as bid_amount_usd,
    b.margin_without_discount                                                         as bid_margin,
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd) 
        * b.margin_without_discount                                                   as bid_margin_usd,
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd) 
        * (a.base_margin_without_discount - b.margin_without_discount)                as bid_margin_loss_usd,        
    b.design_modification_text                                                        as design_modification_text,
    case when b.uuid = a.winning_bid_uuid then true else false end                    as is_winning_bid,

    -- Auction Fields
    sa.auction_uuid                                                                   as auction_uuid,
    a.auction_created_at                                                              as auction_created_at,
    a.order_uuid                                                                      as order_uuid,
    a.winning_bid_uuid                                                                as auction_winning_bid_uuid,
    a.status                                                                          as auction_status,
    a.finished_at                                                                     as auction_finished_at,
    a.auction_document_number                                                         as auction_document_number,
    a.base_margin_without_discount                                                    as auction_base_margin,

    -- Order Level Fields
    (ali.li_subtotal_amount_usd - ali.discount_cost_usd)     as auction_quote_amount_usd

from stg_supplier_auctions as sa
    inner join {{ ref('auctions_rda') }} as a on a.auction_uuid = sa.auction_uuid -- Filter for only RDA Auctions
    left join (select * from bids where rn=1) as b on b.sa_uuid = sa.sa_uuid
    left join {{ ref ('agg_line_items')}} as ali on a.quote_uuid = ali.quote_uuid
    left join {{ ref ('stg_orders_documents')}} as sod on sod.order_uuid = a.order_uuid
