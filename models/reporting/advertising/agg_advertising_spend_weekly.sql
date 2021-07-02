{{
    config(
        materialized = 'incremental'
    )
}}

select date_trunc('week', date)                                            as date,
       source,
       account_id,
       campaign_id,
       campaign_name,
       adgroup_id,
       adgroup_name,
       keyword_id,
       keyword,
       campaign_network,
       campaign_language,
       campaign_market,
       campaign_market_grouped,
       campaign_group_detailed,
       campaign_group,
       sum(impressions)                                                    as impressions,
       sum(clicks)                                                         as clicks,
       sum(cost_usd)                                                       as cost_usd,
       sum(cost_orginal_currency)                                          as cost_orginal_currency,
       sum(impressions * nvl(historical_quality_score, 0))::float /
       nullif(sum(impressions * ((historical_quality_score > 0)::int)), 0) as historical_quality_score,
       sum(impressions * nvl(historical_ad_relevance, 0))::float /
       nullif(sum(impressions * ((historical_ad_relevance > 0)::int)), 0)  as historical_ad_relevance,
       sum(impressions * nvl(historical_expected_ctr, 0))::float /
       nullif(sum(impressions * ((historical_expected_ctr > 0)::int)), 0)  as historical_expected_ctr,
       sum(impressions * nvl(historical_landingpage_experience, 0))::float /
       nullif(sum(impressions * ((historical_landingpage_experience > 0)::int)),
              0)                                                           as historical_landingpage_experience

from {{ ref('agg_advertising_spend_daily') }}

{% if is_incremental() %}

       -- First assess whether week is complete. The below turns `true` when the _next_ day is
       -- in another week.
       where case when date_trunc('week', "date") != date_trunc('week', "date" + 1) then true end
       -- Then incrementally load latest week data
       and date_trunc('week', date) > (select max(date_trunc('week', date)) from {{ this }} )

{% endif %}

{{ dbt_utils.group_by(n=15) }}