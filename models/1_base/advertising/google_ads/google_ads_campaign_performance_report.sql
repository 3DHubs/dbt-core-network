{{ config(bind=False, materialized='table') }}

with campaign_performance_report_ranked as
        ( with prep_campaign_performance as (
             select customerid,
                    campaign,
                    campaignid,
                    clicks,
                    currency,
                    device,
                    cost,
                    day as date,
                    impressions,
                    _sdc_batched_at
             from {{ source('ext_adwords', 'campaign_performance_report') }} -- JG old stitch source till April 27th 2022
             union all
             select customer_id,
                    campaign_name,
                    campaign_id,
                    clicks,
                    customer_currency_code,
                    device,
                    cost_micros,
                    date,
                    impressions,
                    _sdc_batched_at
             from {{ source('ext_google_ads_console', 'campaign_performance_report') }}) -- JG new stitch source from April 27th 2022
             select *, row_number()
                    over (partition by date, campaignid, customerid, device order by _sdc_batched_at desc) as row_number 
             from prep_campaign_performance    
         )
select customerid                                          as account_id,
       campaign                                            as campaign_name,
       campaignid                                          as campaign_id,
       clicks,
       device,
       (cost / 1000000.0)::decimal(9, 2)                   as cost_orginal_currency,
       (cost_orginal_currency / rates.rate)::decimal(9, 2) as cost_usd,
       c.date,
       impressions
from campaign_performance_report_ranked c
         left join "analytics"."data_lake"."exchange_rate_spot_daily" as rates
                   on rates.currency_code_to = c.currency
                       and trunc(c.date) = trunc(rates.date)
where row_number = 1