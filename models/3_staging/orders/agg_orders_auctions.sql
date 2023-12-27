select 
    pa.order_uuid,
    bool_or(case when pa.winning_bid_uuid is not null then true else false end) has_winning_bid_any_auction, -- If any of the auction on an order had a winning bid
    sum(case when pb.supplier_id != coalesce(sod.po_active_supplier_id,-1) then 1 else 0 end) as number_of_auction_cancellations,
    count(1) as number_of_auctions
    from dbt_prod_core.prep_auctions pa
         left join {{ ref('prep_bids') }} pb on pb.uuid = pa.winning_bid_uuid
         left join {{ ref('stg_orders_documents') }} sod on sod.order_uuid = pa.order_uuid
    group by 1
   