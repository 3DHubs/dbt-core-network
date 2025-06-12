select 
    date,
    source,
    account_id,
    campaign_id,
    campaign_name,
    adgroup_id,
    adgroup_name,
    keyword_id,
    keyword,
    _kw_report_sk,
    impressions,
    clicks,
    cost_usd,
    cost_orginal_currency
from {{ ref('fact_search_keyword_performance') }}