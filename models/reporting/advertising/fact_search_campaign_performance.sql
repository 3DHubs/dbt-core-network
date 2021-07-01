{{
    config(
        materialized = 'incremental'
    )
}}

select trunc(acpr.date)                                          as date,
       'adwords'                                                 as source, --TODO: Requires update to Google Ads?
       acpr.account_id,
       acpr.campaign_id,
       coalesce(ac.name, last_value(acpr.campaign_name) over (partition by acpr.campaign_id order by date asc rows
           between unbounded preceding and unbounded following)) as campaign_name, -- coalesce is there for
       -- deleted campaigns, they're also deleted from dbt_prod_data_lake.google_ads_campaigns
       acpr.device,
       acpr.impressions,
       acpr.clicks,
       acpr.cost_usd,
       acpr.cost_orginal_currency

from {{ ref('google_ads_campaign_performance_report') }} as acpr
            left join {{ ref('google_ads_campaigns') }} as ac on ac.id = acpr.campaign_id

where acpr.date >= '2019-07-01'
    and acpr.date < current_date -- from 2019-07-01 we started properly tracking contact source in Hubspot, so data
    -- before this point is not useful

{% if is_incremental() %}

    and trunc(acpr.date) > (select max("date") from {{ this }} )

{% endif %}

union all

select trunc(bcpr.date)                                  as date,
       'bing'                                            as source,
       bcpr.account_id,
       bcpr.campaign_id,
       coalesce(bc.name, last_value(bcpr.campaign_name) over (partition by campaign_id order by date asc rows
           between
           unbounded preceding and unbounded following)) as campaign_name, -- coalesce is there for deleted
       -- campaigns, they're also deleted from `bing_campaigns`
       bcpr.device,
       bcpr.impressions,
       bcpr.clicks,
       bcpr.cost_usd,
        bcpr.cost_orginal_currency

from {{ ref('bing_ads_campaign_performance_report') }} as bcpr
            left join {{ ref('bing_ads_campaigns') }} as bc on bc.id = bcpr.campaign_id

where bcpr.date >= '2019-07-01'
    and bcpr.date < current_date

{% if is_incremental() %}

    and trunc(bcpr.date) > (select max("date") from {{ this }} )

{% endif %}