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

from {{ ref('fact_advertising_spend_daily') }}

-- The below is_incremental() works well (in theory) but the pitfall is that the data
-- from the daily ad spend may be incomplete. Thus, even if the week is complete and
-- we are ready to add another week's of data, it is enough. Instead, we should be
-- looking whether data from daily ad spend is complete for a week and only then
-- insert that new week's worth of data into the weekly ad spend table. For now, I am
-- proposing a "quick" fix by just materializing this model from the ground up each
-- run. It is not efficient, but giving my limited time that's the best I can do now.

-- {% if is_incremental() %}

--        -- First assess whether week is complete. The below turns `true` when the _next_ day is
--        -- in another week.
--        where case when date_trunc('week', current_date) != date_trunc('week', current_date + 1) then true end
--        -- Then incrementally load latest week data
--        and date_trunc('week', "date") > (select max(date_trunc('week', "date")) from {{ this }} )

-- {% endif %}

{{ dbt_utils.group_by(n=15) }}