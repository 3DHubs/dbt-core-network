{{
       config(
              materialized = 'incremental',
              unique_key = '_kw_report_sk'
       )
}}

-- Need pre_hook macro to purge duplicates first. Once this has run on the full
-- data set, it can be run for the last 31 days only as Stitch only fetches l30d
-- data. If we have the pre_hook in place, we can omit the `row_number()` CTE.
 
with keywords_performance_report_ranked as (
       select *,
              {{ dbt_utils.surrogate_key(['day', 'keywordid', 'adgroupid', 'customerid']) }} as _kw_report_sk,
              row_number() over (
                     partition by day, keywordid, adgroupid, customerid
                     order by _sdc_report_datetime desc, _sdc_sequence desc
              ) as row_number
       
       from {{ source('ext_adwords', 'keywords_performance_report') }}

       {% if is_incremental() %}

              where day >= current_date - 31

       {% endif %}
)

select _kw_report_sk,
       customerid                                                       as account_id,
       account                                                          as account_name,
       adgroup                                                          as adgroup_name,
       adgroupid                                                        as adgroup_id,
       adgroupstate                                                     as adgroup_status,
       allconv                                                          as all_conversions,
       campaign                                                         as campaign_name,
       campaignid                                                       as campaign_id,
       campaignstate                                                    as campaign_status,
       clicks,
       conversions,
       (cost / 1000000.0)::decimal(9, 2)                                as cost_orginal_currency,
       (cost_orginal_currency / rates.rate)::decimal(9, 2)              as cost_usd,
       currency                                                         as orginal_currency,
       day                                                              as date,
       decode(adrelevancehist,
              'Above average', 3,
              'Average', 2,
              'Below average', 1,
              null)                                                     as historical_ad_relevance,
       decode(expectedclickthroughratehist,
              'Above average', 3,
              'Average', 2,
              'Below average', 1,
              null)                                                     as historical_expected_ctr,
       decode(expectedclickthroughratehist,
              'Above average', 3,
              'Average', 2,
              'Below average', 1,
              null)                                                     as historical_landingpage_experience,
       qualscorehist                                                    as historical_quality_score,
       (firstpagecpc / 1000000.0)::decimal(9, 2)                        as firstpage_cpc_orginal_currency,
       (firstpage_cpc_orginal_currency / rates.rate)::decimal(9, 2)     as firstpage_cpc_usd,
       (firstpositioncpc / 1000000.0)::decimal(9, 2)                    as firstposition_cpc_orginal_currency,
       (firstposition_cpc_orginal_currency / rates.rate)::decimal(9, 2) as firstposition_cpc_usd,
       impressions,
       keyword,
       keywordid                                                        as keyword_id,
       keywordstate                                                     as keyword_status,
       labelids                                                         as label_ids,
       labels                                                           as labels,
       landingpageexperience                                            as landingpage_experience,
       qualityscore                                                     as quality_score,
       searchabstopis                                                   as seach_absolute_top_impression_share,
       searchimprshare                                                  as seach_impression_share,
       searchtopis                                                      as search_top_impression_share,
       (topofpagecpc / 1000000.0)::decimal(9, 2)                        as top_of_page_cpc_orginal_currency,
       (top_of_page_cpc_orginal_currency / rates.rate)::decimal(9, 2)   as top_of_page_cpc_usd,
       viewthroughconv                                                  as view_trough_conversions

from keywords_performance_report_ranked

         left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                   on rates.currency_code_to = keywords_performance_report_ranked.currency
                       and trunc(keywords_performance_report_ranked.day) = trunc(rates.date)

where row_number = 1