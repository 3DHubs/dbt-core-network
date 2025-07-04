{{
    config(
        materialized = 'incremental',
        unique_key = '_kw_report_sk'
    )
}}

with keywords_performance_report_ranked as
(
    select
        *,
        {{ dbt_utils.surrogate_key(['timeperiod', 'keywordid', 'adgroupid', 'accountid']) }} as _kw_report_sk,
        row_number() over (
            partition by timeperiod, keywordid, adgroupid, accountid
            order by _sdc_report_datetime desc, _sdc_sequence desc
        ) as row_number
    from {{ ref('bing_ads_keywords_source') }}

    {% if is_incremental() %}

            where timeperiod >= current_date - 31

    {% endif %}
)

select
    _kw_report_sk,
    source,
    accountid::bigint as account_id,
    accountname as account_name,
    adgroupid as adgroup_id,
    adgroupname as adgroup_name,
    campaignid as campaign_id,
    campaignname as campaign_name,
    clicks,
    spend::decimal(9, 2) as cost_orginal_currency,
    currencycode as orginal_currency,
    impressions,
    keyword,
    keywordid as keyword_id,
    timeperiod as date,
    (
        cost_orginal_currency / exchange_rate_spot_daily.rate
    )::decimal(9, 2) as cost_usd

from keywords_performance_report_ranked

left join {{ ref('exchange_rate_daily') }} as exchange_rate_spot_daily
    on
        -- todo-migration: replaced the trunc for date_trunc('day', timestamp), check values
        exchange_rate_spot_daily.currency_code_to = keywords_performance_report_ranked.currencycode
        and 
        date_trunc('day', keywords_performance_report_ranked.timeperiod) 
            = date_trunc('day', exchange_rate_spot_daily.date)

        -- trunc(
        --     keywords_performance_report_ranked.timeperiod
        -- ) = trunc(exchange_rate_spot_daily.date)
where row_number = 1
