{{
    config(
        materialized='table',
        sort=['campaign_id', 'date'],
        dist='date'
    )
}}

with campaign_performance_report_ranked as
(
    select
        *,
        row_number() over (
            partition by
                     timeperiod, campaignid, customerid, devicetype
            order by _sdc_report_datetime desc
        ) as row_number

    from {{ source('ext_bing', 'campaign_performance_report') }}
-- Stitch does append-only loading of data for the report tables. Since each report is updated every day for 30 days after the the day, we need to select the latest record only
)

select
    customerid::bigint as account_id,
    accountname as account_name,
    allconversions as all_conversions,
    campaignname as campaign_name,
    campaignid as campaign_id,
    clicks,
    devicetype as device,
    conversions,
    spend::decimal(9, 2) as cost_orginal_currency,
    currencycode as orginal_currency,
    timeperiod as date,
    impressions,
    campaignlabels as labels,
    absolutetopimpressionsharepercent as seach_absolute_top_impression_share,
    impressionsharepercent as seach_impression_share,
    topimpressionsharepercent as search_top_impression_share,
    viewthroughconversions as view_trough_conversions,
    (
        cost_orginal_currency / exchange_rate_spot_daily.rate
    )::decimal(9, 2) as cost_usd

from campaign_performance_report_ranked

left join {{ ref('exchange_rate_daily') }} as exchange_rate_spot_daily
    on
        exchange_rate_spot_daily.currency_code_to = campaign_performance_report_ranked.currencycode
        and trunc(
            campaign_performance_report_ranked.timeperiod
        ) = trunc(exchange_rate_spot_daily.date)

where row_number = 1
