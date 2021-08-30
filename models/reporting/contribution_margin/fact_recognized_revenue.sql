-- Fact Recognized Revenue

-- This tables queries from fact invoices and is later combined with costs in the
-- model fact_contribution_margin. Invoices with a negative sign are credit memos.

select invoice_uuid,
       date_trunc('day', invoice_revenue_date)                           as revenue_recognized_date,
       order_uuid                                                        as order_uuid,
       round(invoice_subtotal_price_amount_usd, 2)::decimal(15, 2)       as recognized_revenue_usd,
       round(coalesce(order_shipping_revenue_usd, 0), 2)::decimal(15, 2) as shipping_revenue_usd,
       round(invoice_subtotal_price_amount, 2)::decimal(15, 2)           as recognized_revenue_source_currency,
       invoice_currency_code                                             as invoice_source_currency,
       invoice_document_number
from {{ ref('fact_invoices') }}
where true
    and invoice_is_recognised is true
    and invoice_status !~ 'processing' -- Processing = draft
