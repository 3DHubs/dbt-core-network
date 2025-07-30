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
       with prep_keyword_performance_report as 
       (select customerid,
       adgroup,
       adgroupid,
       campaign,
       campaignid,
       clicks,
       cost,
       currency,
       day,
       impressions,
       keyword,
       keywordid,
       _sdc_batched_at,
       _sdc_sequence
       
       from {{ source('ext_adwords', 'keywords_performance_report') }}
       where day < '2022-01-01'
       {% if is_incremental() %}

              and day >= current_date - 31

       {% endif %}
       union all
select customer_id,
       ad_group_name,
       ad_group_id,
       campaign_name,
       campaign_id,
       clicks,
       cost_micros,
       customer_currency_code,
       date,
       impressions,
       ad_group_criterion_keyword__text,
       ad_group_criterion_criterion_id,
       _sdc_batched_at,
       _sdc_sequence
       from {{ source('ext_google_ads_console', 'keywords_performance_report') }}
       where date >='2022-01-01'
       {% if is_incremental() %}

              and date >= current_date - 31

       {% endif %}
       ) select *,
              {{ dbt_utils.surrogate_key(['day', 'keywordid', 'adgroupid', 'customerid']) }} as _kw_report_sk,
              row_number() over (
                     partition by day, keywordid, adgroupid, customerid, cost, impressions
                     order by _sdc_batched_at desc, _sdc_sequence desc
              ) as row_number
       from prep_keyword_performance_report
)

select _kw_report_sk,
       customerid                                                       as account_id,
       adgroup                                                          as adgroup_name,
       adgroupid                                                        as adgroup_id,
       campaign                                                         as campaign_name,
       campaignid                                                       as campaign_id,
       clicks,
       (cost / 1000000.0)::decimal(9, 2)                                as cost_orginal_currency,
       (cost_orginal_currency / rates.rate)::decimal(9, 2)              as cost_usd,
       currency                                                         as orginal_currency,
       day                                                              as date,
       impressions,
       keyword,
       keywordid                                                        as keyword_id
from keywords_performance_report_ranked
       left join {{ ref('exchange_rate_daily') }} as rates
              on rates.currency_code_to = keywords_performance_report_ranked.currency
                     and date_trunc('day', cast(keywords_performance_report_ranked.day as timestamp)) = date_trunc('day', cast(rates.date as timestamp)) --todo-migration: trunc changed to date_trunc, do validation
where row_number = 1