{{
    config(
        materialized='table',
        sort=['keyword_id', 'adgroup_id', 'campaign_id'],
        dist='date'
    )
}}

with keywords_performance_report_ranked as
    (
        select *,
                row_number() over (partition by timeperiod, keywordid, adgroupid, accountid order by _sdc_report_datetime desc) as row_number
        from ext_bing.keyword_performance_report
    ) 
select accountid::bigint as account_id,
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
        spend::decimal(9,2) as cost_orginal_currency,
        (cost_orginal_currency / rates.rate)::decimal(9,2) as cost_usd,
        currencycode as orginal_currency,
        currentmaxcpc as current_max_cpc_orginal_currency,
        (current_max_cpc_orginal_currency / rates.rate)::decimal(9,2) as current_max_cpc_usd,
        nullif(historicaladrelevance, '--')::int as historical_ad_relevance,
        nullif(historicalexpectedctr, '--')::int as historical_expected_ctr,
        nullif(historicallandingpageexperience, '--')::int as historical_landingpage_experience,
        nullif(historicalqualityscore, '--')::int as historical_quality_score,
        impressions,
        keyword,
        keywordid as keyword_id,
        keywordlabels as labels,
        keywordstatus as keyword_status,
        landingpageexperience as landingpage_experience,
        qualityscore as quality_score,
        timeperiod as date,
        viewthroughconversions as view_trough_conversions,
        mainlinebid as top_of_page_cpc_orginal_currency,
        (top_of_page_cpc_orginal_currency / rates.rate)::decimal(9,2) as top_of_page_cpc_usd,
        mainline1bid as firstposition_cpc_orginal_currency,
        (firstposition_cpc_orginal_currency / rates.rate)::decimal(9,2) as firstposition_cpc_usd,
        nullif(firstpagebid,'--')::double precision as firstpage_cpc_orginal_currency,
        (firstpage_cpc_orginal_currency / rates.rate)::decimal(9,2) as firstpage_cpc_usd
from keywords_performance_report_ranked
left join analytics.data_lake.exchange_rate_spot_daily as rates
    on rates.currency_code_to = keywords_performance_report_ranked.currencycode
    and trunc(keywords_performance_report_ranked.timeperiod) = trunc(rates.date)
where row_number = 1