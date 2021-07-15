{{
    config(
        materialized = 'incremental'
    )
}}

with campaign_costs_from_keywords as (
         select date,
                source,
                campaign_id,
                sum(cost_orginal_currency) as cost_orginal_currency,
                sum(cost_usd)              as cost_usd,
                sum(impressions)           as impressions,
                sum(clicks)                as clicks
         
         from {{ ref('fact_search_keyword_performance') }}
         
         {% if is_incremental() %}

             where trunc("date") > (select max("date") from {{ this }} )
  
         {% endif %}

         group by 1, 2, 3
         ),

     campaign_costs as (
         select date,
                source,
                account_id,
                campaign_id,
                campaign_name              as campaign_name,
                sum(impressions)           as impressions,
                sum(clicks)                as clicks,
                sum(cost_usd)              as cost_usd,
                sum(cost_orginal_currency) as cost_orginal_currency

         from {{ ref('fact_search_campaign_performance') }}

         {% if is_incremental() %}

             where trunc("date") > (select max("date") from {{ this }} )
  
         {% endif %}

         group by 1, 2, 3, 4, 5
         ),

     stg_costs_without_keywords as (
        select cc.date,
                cc.source,
                cc.account_id,
                cc.campaign_id,
                cc.campaign_name,
                null::int                                                     as ad_group_id,
                null                                                          as adgroup_name,
                null::int                                                     as keyword_id,
                null                                                          as keyword,
                cc.impressions - nvl(ccfk.impressions, 0)                     as impressions,
                cc.clicks - nvl(ccfk.clicks, 0)                               as clicks,
                cc.cost_orginal_currency - nvl(ccfk.cost_orginal_currency, 0) as cost_orginal_currency,
                cc.cost_usd - nvl(ccfk.cost_usd, 0)                           as cost_cost_usd,
                null::int                                                     as historical_quality_score,
                null::int                                                     as historical_ad_relevance,
                null::int                                                     as historical_expected_ctr,
                null::int                                                     as historical_landingpage_experience,
                null::varchar                                                 as _kw_report_sk

        from campaign_costs cc
                left outer join campaign_costs_from_keywords ccfk
                                on (cc.date = ccfk.date and cc.campaign_id = ccfk.campaign_id and cc.source = ccfk.source)

        where cc.cost_orginal_currency - nvl(ccfk.cost_orginal_currency, 0) not between -0.03 and 0.03

        {% if is_incremental() %}

             and trunc(cc."date") > (select max("date") from {{ this }} )
  
        {% endif %}
     ),
    
      stg_cost_union as (
             select * from {{ ref('fact_search_keyword_performance') }}

             union all

             select * from stg_costs_without_keywords
             )

select *,
       split_part(regexp_replace(campaign_name, '^-', '_', 6), '_', 1) as campaign_network,
       split_part(regexp_replace(campaign_name, '^-', '_', 6), '_', 2) as campaign_language,
       split_part(regexp_replace(campaign_name, '^-', '_', 6), '_', 3) as campaign_market,
       case
              when campaign_market in ('US', 'CA', 'US-CA', 'USCA') then 'US/CA'
              when campaign_market in ('GB', 'IE', 'UKI') then 'UKI'
              when campaign_market in ('DACH', 'DE', 'AT', 'CH') then 'DACH'
              when campaign_market in ('FR') then 'FR'
              when campaign_market in ('IT') then 'IT'
              when campaign_market in ('NEUR', 'BE', 'NL', 'BN', 'DK', 'FI', 'NO', 'SE') then 'NEUR'
              else 'Other' end                                         as campaign_market_grouped,
       split_part(regexp_replace(campaign_name, '^-', '_', 6), '_', 4) as campaign_group_detailed,
       case
              when campaign_network in ('DN', 'VN') then 'Display'
              when campaign_name like '%cnc-machining%' then 'CNC'
              when campaign_name like '%injection-molding%' then 'IM'
              when campaign_name like '%3d-printing%' then '3DP'
              when campaign_name like '%competitors%' then 'Competitors'
              when campaign_name like '%manufacturing%' then 'Manufacturing'
              when campaign_name like '%branded%' then 'Brand'
              when campaign_name like '%sheet-metal%' then 'SM'
              else 'Other' end                                         as campaign_group

from stg_cost_union

{% if is_incremental() %}

       where trunc("date") > (select max("date") from {{ this }} )

{% endif %}