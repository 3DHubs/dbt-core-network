select po_uuid                            as source_uuid,
        po_document_number                 as source_document_number,
        'cost'                             as type,
        order_uuid,
        trunc(cost_recognized_date)        as recognized_date,
        source_currency,
        -1 * cost_usd                      as amount_usd,
        -1 * cost_source_currency          as amount_source_currency,
        -1 * cost_shipping_usd             as shipping_usd
from {{ ref('fact_cost_of_goods_sold') }}
union all
select  invoice_uuid                       as source_uuid,
        invoice_document_number            as source_document_number,
        'revenue'                          as type,
        order_uuid,
        trunc(revenue_recognized_date)     as recognized_date,
        invoice_source_currency            as source_currency,
        recognized_revenue_usd             as amount_usd,
        recognized_revenue_source_currency as amount_source_currency,
        shipping_revenue_usd               as shipping_usd
from {{ ref('fact_recognized_revenue') }}