-- ---------------------
-- Production component
-- ---------------------
-- Created by: Daniel Salazar Soplapuco
-- Maintained by: Daniel Salazar Soplapuco
-- Last updated: December 2022
-- Use case:
-- This table determines per order how long it spent in production.
with stg_purcahse_orders as (
    select 
        ppo.order_uuid,
        ppo.created,
        psd.document_number,
        psd.shipping_date,
        ppo.supplier_id,
        psd.finalized_at

    from{{ ref('prep_purchase_orders') }} as ppo
    left join {{ ref('prep_supply_documents') }} as psd on ppo.uuid = psd.uuid
    where psd.type = 'purchase_order'
), production_change_impact as (
        -- Determine the impact of production po changes on allocated production time
        select
            spo.order_uuid,
            'Production' as otr_source,
            'Production Time' as otr_impact,
            'PO promised shipping date adjustments' as otr_process,
            spo.finalized_at,
            spo.document_number,
            spo.shipping_date as current_promised_by_date,
            spo.supplier_id,

            coalesce(
                lag(spo.shipping_date, 1) over (
                    partition by spo.order_uuid order by spo.finalized_at asc
                ),
                current_promised_by_date
            ) as previous_promised_by_date,
            sod.po_first_supplier_id as original_supplier_id,

            date_diff(
                'minute', previous_promised_by_date, current_promised_by_date
            )/60.0 as hours_in_type,
            case
                when supplier_id != original_supplier_id
                then 'Resourced promised by date change'
                when hours_in_type > 0
                then 'Po promised by date increase'
                when hours_in_type < 0
                then 'Po promised by date decrease'
                else 'Po changes without impact'
            end as notes

        from stg_purcahse_orders as spo
        left join
            {{ ref('stg_orders_documents') }} as sod on spo.order_uuid = sod.order_uuid
        where spo.finalized_at is not null
        order by spo.created asc
    )
select
    pci.order_uuid,
    pci.otr_source,
    pci.otr_impact,
    pci.otr_process,
    'Purchase Order' as related_document_type,
    pci.document_number as related_record,
    pci.previous_promised_by_date as start_date,
    pci.current_promised_by_date as end_date,
    pci.hours_in_type,
    hours_in_type as hours_late,
    pci.notes
from production_change_impact as pci

union

-- Determine the impact of label creation
select
    sod.order_uuid,
    'Production' as otr_source,
    'Production Delay' as otr_impact,
    'Label creation' as otr_process,
    'Tracking Number' as related_document_type,
    '' as related_record,
    sod.po_active_promised_shipping_at_by_supplier as start_date,
    sol.shipped_at as end_date,
    date_diff('minute', start_date, end_date)/60.0 as hours_in_type,
    hours_in_type as hours_late,
    case
        when hours_in_type < 0 then 'Supplier created label on time' 
        when sol.shipped_at is null then 'No shipment was found in platform'
        else 'Supplier created label not on time'
    end as notes
from {{ ref('stg_orders_documents') }} as sod
left join {{ ref('stg_orders_logistics') }} as sol on sod.order_uuid = sol.order_uuid

union

-- Determine the impact of pick up discrepancy
select
    sod.order_uuid,
    'Production' as otr_source,
    'Production Delay' as otr_impact,
    'Supplier pick up discrepancy' as otr_sub_typ,
    'Tracking Number' as related_document_type,
    '' as related_record,
    sol.shipped_at as start_date,
    sol.shipment_received_by_carrier_at as end_date,
    date_diff(
        'minute', sol.shipped_at, sol.shipment_received_by_carrier_at
    )/60.0 as hours_in_type,
    hours_in_type as hours_late,
    case
        when hours_in_type > 0
        then 'Supplier created label not on time'
        when hours_in_type = 0
        then 'Supplier created label on time'
        else 'Supplier provided platform label after pickup'
    end as notes
from {{ ref('stg_orders_documents') }} as sod
left join {{ ref('stg_orders_logistics') }} as sol on sod.order_uuid = sol.order_uuid

-- union

-- Determine the originally allocated time for production

union

select
    fo.order_uuid,
    'Production' as otr_source,
    'Production Time' as otr_impact,
    'Original Production Time' as otr_process,
    'auction uuid' as related_document_type,
    frb.auction_uuid as related_document,
    case
        when frb.auction_uuid is not null and par.first_successful_auction then par.finished_at
        else fo.sourced_at
    end as start_at,
    case
        when frb.auction_uuid is not null then frb.original_ship_by_date
        else sod.po_first_promised_shipping_at_by_supplier
    end as end_date,
    datediff('minute', start_at, end_date) / 60.0 as hours_in_type,
    0 as hours_late,
    'The originally allocated production time excluding counterbid, sourcing time and non business day adjustment' as notes
from {{ ref('fact_orders') }} as fo
left join {{ ref('fact_rda_behaviour') }} as frb on fo.order_uuid = frb.order_uuid and frb.is_winning_bid
left join {{ ref('prep_auctions_rda') }} as par on frb.auction_uuid = par.auction_uuid and par.first_successful_auction
left join {{ ref('stg_orders_documents') }} as sod on fo.order_uuid = sod.order_uuid
where case when frb.auction_uuid is not null then par.first_successful_auction and frb.is_winning_bid else True end