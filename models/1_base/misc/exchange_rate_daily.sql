SELECT 

    "date",
    currency_code_base,
    currency_code_to,
    is_success,
    is_historical,
    rate

FROM {{ source('dbt_ingestion', 'gold_ext_airbyte_exchange_rate_daily') }}
