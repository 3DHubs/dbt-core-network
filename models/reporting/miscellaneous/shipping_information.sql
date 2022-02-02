select
    sci.carrier,
    sci.awb,
    sci.declared_value,
    sci.custom_outlay,
    sci.shipping_cost,
    sci.vat,
    sci.other_costs,
    sci.carrier_invoiced_date,
    sci.entity,
    sci.carrier_invoice_number,
    sci.finance_invoiced_date,
    sci.quote_document_number,
    sci.currency,
    sci.weight_kg,
    oqsl.order_uuid,
    coalesce(ship.shipping_leg, sci.shipping_leg) as shipping_leg_full,
    declared_value + custom_outlay + shipping_cost + vat + other_costs as total_costs,
    coalesce(round(declared_value / ex.rate,2),0) as declared_value_usd,
    coalesce(round(custom_outlay / ex.rate,2),0) as custom_outlay_usd,
    coalesce(round(shipping_cost / ex.rate,2),0) as shipping_cost_usd,
    coalesce(round(vat / ex.rate,2),0) as vat_usd,
    coalesce(round(other_costs / ex.rate,2),0) as other_costs_usd,
    declared_value_usd + custom_outlay_usd + shipping_cost_usd + vat_usd + other_costs_usd as total_costs_usd
from {{ source('int_logistics', 'shipping_customs_information') }} as sci
left join {{ source('int_service_supply', 'cnc_order_quotes') }} as oqsl on sci.quote_document_number = oqsl.document_number
left join {{ source('int_service_supply', 'shipments') }} as ship on ship.tracking_number = sci.awb
left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as ex on sci.currency = ex.currency_code_to and sci.carrier_invoiced_date = trunc(ex.date)
where oqsl.type = 'quote'