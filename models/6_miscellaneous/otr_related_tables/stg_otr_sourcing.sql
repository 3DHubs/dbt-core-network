-- -------------------
-- Sourcing component
-- -------------------
-- Created by: Daniel Salazar Soplapuco
-- Maintained by: Daniel Salazar Soplapuco
-- Last updated: December 2022
-- Use case:
-- This table determines per order how long it spent in sourcing.
-- Sourcing time RDA
select
    par.order_uuid,
    'Sourcing' as otr_source,
    'Sourcing Time' as otr_impact,
    'Sourcing ' + auction_type as otr_process,
    'auction uuid' as related_document_type,
    par.auction_uuid as related_document,
    par.auction_created_at as start_date,
    par.finished_at as end_date,
    datediff('minute', par.auction_created_at, par.finished_at)
    / 60.0 as hours_in_type,
    0 as hours_late,
    'This is the sourcing time used for the order.' as notes
from  {{ ref("prep_auctions") }} as par 
where  par.first_successful_auction


union

-- Sourcing Time when an RFQ failed and the order was put back onto the RDA.
select
    frb.order_uuid,
    'Sourcing' as otr_source,
    'Sourcing Time' as otr_impact,
    'Sourcing Failed RFQ' as otr_process,
    'RFQ' as related_document_type,
    '' as related_document,
    min(sod.closed_at) as start_date,
    min(frb.auction_created_at) as end_date,
    datediff('minute', start_date, end_date) / 60.0 as hours_in_type,
    0 as hours_late,
    'This is additional sourcing time due to the RFQ process failing and the order being placed on the RDA.' as notes
from {{ ref("fact_auction_behaviour") }} as frb
left join {{ ref("stg_orders_dealstage") }} as sod on frb.order_uuid = sod.order_uuid
left join {{ ref("prep_auctions") }} as parfq on frb.order_uuid = parfq.order_uuid and parfq.is_rfq
left join {{ ref("prep_auctions") }} as parda on frb.auction_uuid = parda.auction_uuid and not parda.is_rfq

where 
    parfq.auction_created_at < frb.auction_created_at
    and frb.is_winning_bid and not frb.is_rfq
    and parda.first_successful_auction
    and parfq.is_rfq
group by 1, 2, 3, 4, 5

union

-- Counter bid on lead time adjustment
select
    frb.order_uuid,
    'Sourcing' as otr_source,
    'Production Time' as otr_impact,
    'Counter bid on lead time' as otr_process,
    'auction uuid' as related_document_type,
    frb.auction_uuid as related_document,
    frb.original_ship_by_date as start_date,
    frb.accepted_ship_by_date as end_date,
    datediff('minute', frb.original_ship_by_date, frb.accepted_ship_by_date)
    / 60.0 as hours_in_type,
    hours_in_type as hours_late,
    'This is the adjustment as a result of a counterbid on lead time.' as notes
from {{ ref("fact_auction_behaviour") }} as frb
left join {{ ref("prep_auctions") }} as par on frb.auction_uuid = par.auction_uuid and not par.is_rfq 
where frb.is_winning_bid and par.first_successful_auction and not frb.is_rfq

-- union

-- -- Counter bid sourcing time adjustment
-- select
--     frb.order_uuid,
--     'Sourcing' as otr_source,
--     'Production Time' as otr_impact,
--     'Counter bid sourcing time adjustment' as otr_process,
--     'auction uuid' as related_document_type,
--     frb.auction_uuid as related_document,
--     par.started_at as start_date,
--     par.finished_at as end_date,
--     case
--         when datediff('minute', frb.accepted_ship_by_date, frb.ship_by_date) > 0
--         then datediff('minute', par.started_at, par.finished_at) / 60.0
--         else 0
--     end as hours_in_type,
--     hours_in_type as hours_late,
--     'This is an ajustment occurs to account for the sourcing time after a counterbid is accepted after the closing of the sourcing window.'as notes
-- from {{ ref("fact_auction_behaviour") }} as frb
-- left join {{ ref("prep_auctions") }} as par on frb.auction_uuid = par.auction_uuid and not par.is_rfq
-- where frb.is_winning_bid and par.first_successful_auction and not frb.is_rfq

-- union

-- Counter bid non business day adjustment
-- select
--     frb.order_uuid,
--     'Sourcing' as otr_source,
--     'Production Time' as otr_impact,
--     'Counter bid non business day adjustment' as otr_process,
--     'auction uuid' as related_document_type,
--     frb.auction_uuid as related_document,
--     null as start_date,
--     null as end_date,
--     case
--         when datediff('minute', frb.accepted_ship_by_date, frb.ship_by_date) > 0 then
--             (datediff('minute', frb.accepted_ship_by_date, frb.ship_by_date) - coalesce(datediff('minute', par.started_at, par.finished_at),0))
--             / 60.0
--         else 0
--     end as hours_in_type,
--     hours_in_type as hours_late,
--     'This is an ajustment occurs to prevent the supplier promised by date of falling on a weekend in the event of a counterbid.' as notes
-- from {{ ref("fact_auction_behaviour") }} as frb
-- left join {{ ref("prep_auctions") }} as par on frb.auction_uuid = par.auction_uuid and not par.is_rfq
-- where frb.is_winning_bid and par.first_successful_auction and not frb.is_rfq
