
with akpr as (
    select *
    from {{ ref('google_ads_keywords_performance_report') }}
    where "date" >= '2019-07-01'
      and "date" < current_date

    -- {% if is_incremental() %}
    -- -- We load the last 30 days because that's how far Stitch looks back at the keyword performance report.

    --     and trunc("date") >= current_date - 30

    -- {% endif %}
),

     bkpr as (
    select *
    from {{ ref('bing_ads_keywords_performance_report') }}
    where date >= '2019-07-01'
    and date < current_date -- from 2019-07-01 we started properly tracking contact source in Hubspot, so data before this point is not useful

        -- {% if is_incremental() %}

        --     and trunc("date") >= current_date - 30

        -- {% endif %}
     ),
combined as (

-- Google data
select trunc(akpr.date)                                      as date,
       'adwords'                                             as source, --TODO: Requires update to Google Ads?
       'adwords'                                             as sub_source,
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
       akpr._kw_report_sk

from akpr
            left join {{ ref ('google_ads_campaigns') }} as ac on ac.id = akpr.campaign_id
            left join {{ ref ('google_ads_ad_groups') }} as aag on aag.id = akpr.adgroup_id

union all

-- Bing data
select trunc(bkpr.date)                                          as date,
       'bing'                                                    as source,
       bkpr.source                                               as sub_source,
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
       bkpr._kw_report_sk

from bkpr
            left join {{ ref ('bing_ads_campaigns') }} as bc on bc.id = bkpr.campaign_id and bc.source = bkpr.source
            left join {{ ref ('bing_ads_ad_groups') }} as bad on bad.id = bkpr.adgroup_id and bad.source = bkpr.source)
select date,
       source,
       sub_source,
       account_id,
       campaign_id,
       campaign_name,
       adgroup_id,
       adgroup_name,
       keyword_id,
       keyword,
       _kw_report_sk,
       sum(impressions) impressions,
       sum(clicks) clicks,
       sum(cost_usd) cost_usd,
       sum(cost_orginal_currency) as cost_orginal_currency
 from combined
 group by 1,2,3,4,5,6,7,8,9,10,11