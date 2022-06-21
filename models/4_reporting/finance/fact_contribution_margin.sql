{{ config(
    tags=["notmultipledayrefresh"]
) }}
select  fpo.po_uuid                            as source_uuid,
        fpo.po_document_number                 as source_document_number,
        'cost'                                 as type,
        fpo.order_uuid,
        trunc(fpo.cost_recognized_at)        as recognized_date,
        fpo.source_currency,
        -1 * fpo.cost_usd                      as amount_usd,
        -1 * fpo.cost_source_currency          as amount_source_currency,
        -1 * fpo.cost_shipping_usd             as shipping_usd
from {{ ref('fact_purchase_orders') }} as fpo
where fpo.cogs_is_recognized
union all
select  invoice_uuid                       as source_uuid,
        invoice_document_number            as source_document_number,
        'revenue'                          as type,
        order_uuid,
        trunc(revenue_recognized_at)       as recognized_date,
        invoice_source_currency            as source_currency,
        revenue_usd                        as amount_usd,
        revenue_source_currency            as amount_source_currency,
        shipping_revenue_usd               as shipping_usd
from {{ ref('fact_invoices') }}
where revenue_is_recognized