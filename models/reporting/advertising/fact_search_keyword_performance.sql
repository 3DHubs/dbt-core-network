{{
    config(
        materialized = 'incremental'
    )
}}

-- Google data
select trunc(akpr.date)                                          as date,
       'adwords'                                             as source, --TODO: Requires update to Google Ads?
       akpr.account_id,
       akpr.campaign_id,
       coalesce(ac.name, last_value(akpr.campaign_name) over (partition by akpr.campaign_id order by date asc rows
           between unbounded preceding and unbounded following)) as campaign_name, -- coalesce is there for
       -- deleted campaigns, they're also deleted from dbt_prod_data_lake.google_ads_campaigns
       akpr.adgroup_id,
       coalesce(aag.name, last_value(akpr.adgroup_name) over (partition by akpr.adgroup_id order by date asc rows
           between unbounded preceding and unbounded following)) as adgroup_name,  -- coalesce is there for
       -- deleted adgroups, they're also deleted from dbt_prod_data_lake.google_ad_groups
       akpr.keyword_id,
       akpr.keyword,
       akpr.impressions,
       akpr.clicks,
       akpr.cost_usd,
       akpr.cost_orginal_currency,
       akpr.historical_quality_score,
       akpr.historical_ad_relevance,
       akpr.historical_expected_ctr,
       akpr.historical_landingpage_experience

from {{ ref('google_ads_keywords_performance_report') }} as akpr
            left join {{ ref ('google_ads_campaigns') }} as ac on ac.id = akpr.campaign_id
            left join {{ ref ('google_ads_ad_groups') }} as aag on aag.id = akpr.adgroup_id

where date >= '2019-07-01'
    and date < current_date -- from 2019-07-01 we started properly tracking contact source in Hubspot, so data before this point is not useful

{% if is_incremental() %}

    and trunc(akpr.date) > (select max("date") from {{ this }} )

{% endif %}

union all

-- Bing data
select trunc(bkpr.date)                                          as date,
       'bing'                                                    as source,
       bkpr.account_id,
       bkpr.campaign_id,
       coalesce(bc.name, last_value(bkpr.campaign_name) over (partition by bkpr.campaign_id order by date asc rows
           between unbounded preceding and unbounded following)) as campaign_name, -- coalesce is there for
       -- deleted campaigns, they're also deleted from `bing_campaigns`
       bkpr.adgroup_id,
       coalesce(bad.name, last_value(bkpr.adgroup_name) over (partition by bkpr.adgroup_id order by date asc rows
           between
           unbounded preceding and unbounded following))         as adgroup_name,  -- coalesce is there for
       -- deleted adgroups, they're also deleted from `bing_ad_groups`
       bkpr.keyword_id,
       bkpr.keyword,
       bkpr.impressions,
       bkpr.clicks,
       bkpr.cost_usd,
       bkpr.cost_orginal_currency,
       bkpr.historical_quality_score,
       bkpr.historical_ad_relevance,
       bkpr.historical_expected_ctr,
       bkpr.historical_landingpage_experience

from {{ ref('bing_ads_keywords_performance_report') }} as bkpr
            left join {{ ref ('bing_ads_campaigns') }} as bc on bc.id = bkpr.campaign_id
            left join {{ ref ('bing_ads_ad_groups') }} as bad on bad.id = bkpr.adgroup_id

where date >= '2019-07-01'
    and date < current_date -- from 2019-07-01 we started properly tracking contact source in Hubspot, so data before this point is not useful

{% if is_incremental() %}

    and trunc(bkpr.date) > (select max("date") from {{ this }} )

{% endif %}