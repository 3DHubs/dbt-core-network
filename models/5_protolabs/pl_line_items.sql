{{ config(
    materialized = "view"
) }}

select
    *
    from {{ ref('fact_quote_line_items') }}
    where created_date >= '2019-01-01'

