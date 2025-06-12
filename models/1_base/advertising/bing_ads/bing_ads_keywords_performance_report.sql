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
    accountnumber as account_number,
    accountstatus as account_status,
    adgroupid as adgroup_id,
    adgroupname as adgroup_name,
    adgroupstatus as adgroup_status,
    allconversions as all_conversions,
    averageposition as average_position,
    campaignid as campaign_id,
    campaignname as campaign_name,
    campaignstatus as campaign_status,
    clicks,
    conversions,
    spend::decimal(9, 2) as cost_orginal_currency,
    currencycode as orginal_currency,
    currentmaxcpc as current_max_cpc_orginal_currency,
    impressions,
    keyword,
    keywordid as keyword_id,
    keywordlabels as labels,
    keywordstatus as keyword_status,
    landingpageexperience as landingpage_experience,
    qualityscore as quality_score,
    timeperiod as date,
    viewthroughconversions as view_trough_conversions,
    (
        cost_orginal_currency / exchange_rate_spot_daily.rate
    )::decimal(9, 2) as cost_usd,
    (
        current_max_cpc_orginal_currency / exchange_rate_spot_daily.rate
    )::decimal(9, 2) as current_max_cpc_usd

from keywords_performance_report_ranked

left join {{ ref('exchange_rate_daily') }} as exchange_rate_spot_daily
    on
        exchange_rate_spot_daily.currency_code_to = keywords_performance_report_ranked.currencycode
        and trunc(
            keywords_performance_report_ranked.timeperiod
        ) = trunc(exchange_rate_spot_daily.date)
where row_number = 1
