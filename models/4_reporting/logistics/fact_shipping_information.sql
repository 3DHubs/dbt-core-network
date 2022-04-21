{{ config(
    tags=["adhoc"]
) }}

select
    oqsl.order_uuid,
    sci.carrier,
    sci.invoice_date,
    sci.shipment_date,
    sci.product_name,
    sci.service_category,
    sci.invoice_number,
    sci.shipment_number,
    sci.corporate_country,
    sci.document_number,
    sci.weight_kg,
    sci.recipient_postal_code,
    sci.shipper_postal_code,
    coalesce(round(sci.statistical_value/ ex.rate,2),0) as statistical_value,
    coalesce(ship.shipping_leg, sci.shipping_leg) as shipping_leg_full,
    sci.charge_name,
    sci.charge_category,
    sci.currency,
    coalesce(round(sci.charge_amount / ex.rate,2),0) as charge_amount,
    coalesce(round(sci.total_amount / ex.rate,2),0) as total_costs_usd
from {{ source('int_logistics', 'automated_shipping_customs_information') }} as sci
left join {{ source('int_service_supply', 'cnc_order_quotes') }} as oqsl on sci.document_number = oqsl.document_number
left join {{ source('int_service_supply', 'shipments') }} as ship on ship.tracking_number = sci.shipment_number
left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as ex on sci.currency = ex.currency_code_to and sci.invoice_date = trunc(ex.date)
where oqsl.type = 'quote'