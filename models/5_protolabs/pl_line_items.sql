{{ config(
    tags=["multirefresh"]
) }}

select
    line_item_uuid,
    order_uuid,
    line_item_type,
    is_complaint,
    complaint_is_valid,
    complaint_created_at,
    complaint_resolution_at,
    dispute_created_at,
    complaint_liability,
    complaint_type,
    line_item_price_amount_usd,
    line_item_cost_usd
    from {{ ref('fact_quote_line_items') }}
    where created_date >= '2019-01-01'

