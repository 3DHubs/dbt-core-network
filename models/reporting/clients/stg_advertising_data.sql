-- Warning: hard-coded references to `reporting` model.
with keywords as (
    select distinct source, account_id, campaign_id, adgroup_id, keyword_id, campaign_group
    from {{ source('reporting', 'agg_advertising_spend_monthly') }}
    ),
     campaigns as (
    select distinct source, account_id, campaign_id, campaign_group
    from {{ source('reporting', 'agg_advertising_spend_monthly') }}
    )
select client_id,
       advertising_gclid,
       advertising_msclkid,
       advertising_source,
       coalesce(cpc.customer_id, keywords.account_id, campaigns.account_id)         as advertising_account_id,
       coalesce(cpc.campaign_id, keywords.campaign_id, campaigns.campaign_id)        as advertising_campaign_id,
       coalesce(campaigns.campaign_group, keywords.campaign_group)                  as advertising_campaign_group,
       -- Setting keyword_id and adgroup_id to null for campaign_group = 'Display' here, because we only have campaign-level data on reporting.agg_advertising_spend for Display
       case
           when stg_hsa_keyword_id is not null and coalesce(advertising_campaign_group,'') <> 'Display' then coalesce(cpc.keyword_id, keywords.keyword_id)
           else null end                                                            as advertising_keyword_id,
       case
           when stg_hsa_keyword_id is not null and coalesce(advertising_campaign_group,'') <> 'Display' then coalesce(cpc.adgroup_id, keywords.adgroup_id)
           else null end                                                            as advertising_adgroup_id,
       trunc(
               coalesce(cpc.date, hutk_analytics_first_visit_timestamp::timestamp)) as advertising_click_date,
       lower(coalesce(cpc.device, nullif(json_extract_path_text(dcps.first_page_seen_query, 'device'),
                                           '')))                                      as stg_device,
       case
           when stg_device similar to '%desktop%|%computer%|c' then 'Desktop'
           when stg_device similar to '%tablet%|t' then 'Tablet'
           when stg_device similar to '%mobile%|m' then 'Mobile'
           else stg_device end                                                      as advertising_click_device
from {{ ref('stg_advertising_data_prep') }} as dcps
            left join {{ ref('google_ads_click_performance_report') }} cpc on (dcps.advertising_gclid = cpc.google_click_id)
            left join keywords on (cpc.google_click_id is null and keywords.source = dcps.advertising_source and -- performance optimization: exclude already joined gclds from this join, so bing and some google clicks without gclid are only joined here. This join is expensive!
                                    (keywords.adgroup_id in (dcps.stg_hsa_grp, dcps.stg_utm_content) and
                                      keywords.keyword_id in (dcps.stg_hsa_keyword_id, dcps.stg_utm_term)))
            left join campaigns on ((cpc.customer_id = campaigns.account_id and cpc.campaign_id = campaigns.campaign_id) --join in campaigns for already joined gclids
                                 or ((keywords.keyword_id is null and cpc.google_click_id is null) and campaigns.source = dcps.advertising_source and campaigns.campaign_id in (dcps.stg_hsa_cam, dcps.stg_utm_campaign))) -- join in campaigns for clients that couldn't be joined on the keyword/adgroup level