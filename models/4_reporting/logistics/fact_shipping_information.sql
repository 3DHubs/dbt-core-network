{{ config(
    tags=["adhoc", "daily_exclusion"]
) }}

select
    asci.shipment_number||'-'|| ROW_NUMBER () over (PARTITION BY asci.shipment_number) as synthetic_primary_key,
    coalesce(psd.order_uuid, pso.uuid) as order_uuid,
    asci.carrier,
    asci.invoice_date,
    asci.shipment_date,
    asci.product_name,
    asci.service_category,
    asci.invoice_number,
    asci.shipment_number,
    asci.corporate_country,
    asci.document_number,
    asci.weight_kg,
    asci.recipient_postal_code,
    asci.shipper_postal_code,
    coalesce(round(asci.statistical_value/ ex.rate,2),0) as statistical_value,
    coalesce(asci.shipping_leg, ship.shipping_leg) as shipping_leg_full,
    asci.charge_name,
    lower(asci.charge_category) as charge_category,
    asci.currency,
    asci.charge_amount as source_currency_charge_amount,
    asci.total_amount as source_currency_total_amount,
    coalesce(round(asci.charge_amount / ex.rate,2),0) as charge_amount,
    coalesce(round(asci.total_amount / ex.rate,2),0) as total_costs_usd
from {{ source('int_logistics', 'automated_shipping_customs_information') }} as asci
left join {{ ref('prep_supply_documents') }} as psd on asci.document_number = psd.document_number
left join {{ ref('prep_supply_orders') }} as pso on asci.document_number = pso.number
left join {{ ref('prep_shipments') }} as ship on ship.tracking_number = asci.shipment_number
left join {{ ref('exchange_rate_daily') }} as ex on asci.currency = ex.currency_code_to and asci.invoice_date = date_trunc('day', ex.date) --todo-migration-test: replaced trunc with date_trunc
left join {{ ref('prep_supply_integration') }} as integration on  coalesce(psd.order_uuid, pso.uuid) = integration.order_uuid
where not integration.is_test --todo-migration-test: replaced is not true with not {field}