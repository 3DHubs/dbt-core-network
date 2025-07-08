SELECT 

    date,
    currency_code_base,
    currency_code_to,
    is_success,
    is_historical,
    rate

FROM {{ ref('dbt_src_external', 'gold_airbyte_exchange_rate_daily') }}
