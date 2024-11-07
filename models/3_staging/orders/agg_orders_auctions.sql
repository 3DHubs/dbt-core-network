with prep_winning_bid_auction_type as
    (
    select order_uuid,
        auction_type,
        row_number() over (partition by order_uuid order by auction_created_at desc) as rn
    from {{ ref('prep_auctions') }}
    where winning_bid_uuid is not null
        ),

winning_bid_auction_type as
    (
    select 
        order_uuid,
        auction_type as last_winning_bid_auction_type
    from prep_winning_bid_auction_type
    where rn=1
        )

select 
    pa.order_uuid,
    wat.last_winning_bid_auction_type,
    bool_or(case when pa.winning_bid_uuid is not null then true else false end) has_winning_bid_any_auction, -- If any of the auction on an order had a winning bid
    sum(case when pb.supplier_id != coalesce(sod.po_active_supplier_id,-1) then 1 else 0 end) as number_of_auction_cancellations,
    count(distinct case when auction_type = 'RFQ' then pa.quote_uuid else pa.auction_uuid  end) as number_of_auctions

    from {{ ref('prep_auctions') }} pa
         left join winning_bid_auction_type as wat on wat.order_uuid = pa.order_uuid
         left join {{ ref('bids') }} pb on pb.uuid = pa.winning_bid_uuid
         left join {{ ref('stg_orders_documents') }} sod on sod.order_uuid = pa.order_uuid
    group by 1,2
   