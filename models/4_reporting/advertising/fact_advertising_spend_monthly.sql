select date_trunc('month', date)                                           as date,
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
    
from {{ ref('fact_advertising_spend_daily') }}
    
{{ dbt_utils.group_by(n=15) }}