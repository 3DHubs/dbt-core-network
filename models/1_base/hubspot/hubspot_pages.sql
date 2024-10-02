{{ config(materialized="incremental") }}

with
    final as (
        select
            breakdown as url,
            meta as name,
            date as date,
            entrances as entrances,
            rawviews as pageviews,
            timeperpageview::decimal(18, 2) as time_per_pageview,
            pagebouncerate::decimal(18, 2) as bounce_rate
        from {{ source("dbt_ingestion", "gold_ext_airbyte_hubspot_pages") }}

        {% if is_incremental() %}

            where date not in (select distinct date from {{ this }})

        {% endif %}

    )

select *
from final
