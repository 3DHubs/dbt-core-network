select date_trunc('day', invoice_revenue_date)                       as revenue_recognized_date,
       date_trunc('day', invoice_finalized_at)                       as invoice_finalized_at,
       order_technology_id,
       order_technology_name,
       invoice_uuid,
       invoice_order_uuid                                                as order_uuid,
       is_before_delivery,
       round(invoice_subtotal_price_amount_usd, 2)::decimal(15, 2)       as recognized_revenue_usd,
       round(coalesce(order_shipping_revenue_usd, 0), 2)::decimal(15, 2) as shipping_revenue_usd,
       round(invoice_subtotal_price_amount, 2)::decimal(15, 2)           as recognized_revenue_source_currency,
       round(coalesce(order_shipping_revenue, 0), 2)::decimal(15, 2)     as shipping_revenue_source_currency,
       invoice_currency_code                                             as invoice_source_currency,
       invoice_document_number
from {{ ref('fact_invoices') }}
where true
    and invoice_is_recognized_revenue = 1
    and invoice_status !~ 'processing' -- Processing = draft