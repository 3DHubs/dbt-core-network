{{ config(
    tags=["notmultipledayrefresh"],
) }}

select  fpo.po_uuid                            as source_uuid,
        fpo.po_document_number                 as source_document_number,
        'cost'                                 as type,
        fpo.order_uuid,
        trunc(fpo.cost_recognized_at)          as recognized_date,
        fpo.source_currency,
        fpo.exchange_rate_po                   as exchange_rate,
        fpo.exchange_rate_at_sourcing          as exchange_rate_intial, -- exchange rate at sourcing
        -1 * fpo.cost_usd                      as amount_usd,
        -1 * fpo.cost_source_currency          as amount_source_currency,
        -1 * fpo.cost_shipping_usd             as shipping_usd
from {{ ref('fact_purchase_orders') }} as fpo
where fpo.cogs_is_recognized
union all
select  fi.invoice_uuid                       as source_uuid,
        fi.invoice_document_number            as source_document_number,
        'revenue'                             as type,
        fi.order_uuid,
        trunc(fi.revenue_recognized_at)       as recognized_date,
        fi.invoice_source_currency            as source_currency,
        fi.exchange_rate_invoices             as exchange_rate,
        fi.exchange_rate_at_closing           as exchange_rate_intial, -- exchange rate at closing
        fi.revenue_usd                        as amount_usd,
        fi.revenue_source_currency            as amount_source_currency,
        fi.shipping_revenue_usd               as shipping_usd
from {{ ref('fact_invoices') }} as fi
    where revenue_is_recognized
