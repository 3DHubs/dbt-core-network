{{ config(bind=False, materialized='table') }}

with campaign_performance_report_ranked as
         (
             select *,
                    row_number()
                    over (partition by day, campaignid, customerid, device order by _sdc_report_datetime desc) as row_number
             from {{ source('ext_adwords', 'campaign_performance_report') }}
         )
select customerid                                          as account_id,
       account                                             as account_name,
       allconv                                             as all_conversions,
       campaign                                            as campaign_name,
       campaignid                                          as campaign_id,
       clicks,
       device,
       conversions,
       (cost / 1000000.0)::decimal(9, 2)                   as cost_orginal_currency,
       (cost_orginal_currency / rates.rate)::decimal(9, 2) as cost_usd,
       currency                                            as orginal_currency,
       day                                                 as date,
       impressions,
       labelids                                            as label_ids,
       labels                                              as labels,
       searchabstopis                                      as seach_absolute_top_impression_share,
       searchimprshare                                     as seach_impression_share,
       searchtopis                                         as search_top_impression_share,
       viewthroughconv                                     as view_trough_conversions
from campaign_performance_report_ranked
         left join {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                   on rates.currency_code_to = campaign_performance_report_ranked.currency
                       and trunc(campaign_performance_report_ranked.day) = trunc(rates.date)
where row_number = 1