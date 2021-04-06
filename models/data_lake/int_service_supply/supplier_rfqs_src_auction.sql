/* This is a solution intended to be temporary. There's a data model `supplier_rfqs` that holds similar data. We 
   need to figure out what the differences/similarities are. To be continued... */

select oqs.created,
                         oqs.updated,
                         oqs.deleted,
                         oqs.order_uuid                                                 as order_uuid,
                         auctions.uuid                                                  as order_quotes_uuid,
                         auctions.winner_bid_uuid,
                         auctions.status,                       -- If auction gets status 'resourced' it means it has been brought back to the auction
                         auctions.started_at,
                         auctions.finished_at,
                         auctions.ship_by_date,
                         auctions.last_processed_at,
                         auctions.internal_support_ticket_id,
                         decode(auctions.is_internal_support_ticket_opened, 'true', True, 'false',
                                False)                                                  as is_internal_support_ticket_opened,
                         decode(auctions.china_throttled, 'true', True, 'false', False) as is_china_throttled,
                         auctions.base_margin,                  -- For debugging purposes only, do not use for reporting
                         auctions.base_margin_without_discount, -- This field will be used in auctions
                         auctions.next_allocate_at,
                         decode(auctions.is_resourcing, 'true', True, 'false', False)   as is_resourcing,
                         row_number() over (partition by oqs.order_uuid order by auctions.started_at desc nulls last)
                                                                                        as recency_idx
                  from int_service_supply.auctions as auctions
                           inner join {{ ref('cnc_order_quotes') }} as oqs
                                      on auctions.uuid = oqs.uuid
                  where decode(auctions.is_rfq, 'true', True, 'false', False)