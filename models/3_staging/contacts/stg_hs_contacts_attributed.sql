{{
       config(
              materialized='table',
              pre_hook="analyze {{ ref('stg_hs_contacts_attributed_prep') }}",
              post_hook="analyze {{ this }}"
       )
}}

select coalesce(page_group, 'Ungrouped')                                  as tmp_first_page_seen_grouped,
       case
           when channel_type = 'outbound' then 'outbound'
           when (ip_country_code <> ('us') or ip_country_code is null) and created_at < '2019-07-01'
               then 'unknown_channel_old_row'
           when ip_country_code = 'us' and created_at < '2019-05-01' then 'unknown_channel_old_us'
           -- This detects contacts without a web visit before first party tracking went live
           when datediff(second, hutk_analytics_first_visit_timestamp::timestamp, created_at::timestamp) < 10 and
                created_at < '2021-01-05'
               then 'unknown_channel_no_web_session'
           -- After first party tracking went life contacts with a equal first_visit_timestamp and created_at are expected for contacts with adblockers.
           when datediff(second, hutk_analytics_first_visit_timestamp::timestamp, created_at::timestamp) < 0 and
                created_at >= '2021-01-05'
               then 'unknown_channel_no_web_session'
           when hutk_analytics_source = 'offline' and
                lower(hutk_analytics_source_data_1) in ('analytics', 'api', 'conversations')
               then 'unknown_channel_no_web_session'
           when hutk_analytics_first_url like '%utm_medium=display%' then 'display'
           when hutk_analytics_first_url like '%utm_source=youtube%' then 'youtube'
           when hutk_analytics_source = 'organic_search' and tmp_first_page_seen_grouped = 'Homepage'
               then 'branded_organic_search'
           when hutk_analytics_source = 'direct_traffic' and
                hutk_analytics_first_url = 'https://www.3dhubs.com/manufacture%' and
                created_at <= '2021-03-25' -- after implementing 1st party tracking on /manufacturing on 2021-03-25 this is not needed anymore
               then 'unknown_channel'
           when hutk_analytics_source is null then 'unknown_channel'
            else hutk_analytics_source end                                                as channel, 
       case
           when channel not like 'unknown_channel%' then tmp_first_page_seen_grouped end  as first_page_seen_grouped,
       case
           when channel not like 'unknown_channel%' then hutk_analytics_first_page end    as first_page_seen,
       case
           when channel not like 'unknown_channel%' then substring(nullif(regexp_substr(hutk_analytics_first_url, '\\?.*'), ''),
                                                   2) end                 as first_page_seen_query,
       nullif(query_to_json(first_page_seen_query), '')       as first_page_seen_query_tmp,
       case
           when channel not like 'unknown_channel%' then hutk_analytics_source_data_1 end as channel_drilldown1,
       case
           when channel not like 'unknown_channel%' then hutk_analytics_source_data_2 end as channel_drilldown2,
        case
            when left(channel, 7) = 'unknown' or channel is null then 'unknown'
            when channel = 'display' then 'display'
            when channel = 'youtube' then 'youtube'
            when channel in ('social_media', 'referrals', 'other_campaigns', 'email_marketing') then 'other'
            when channel in ('branded_organic_search', 'direct_traffic') then 'direct/brand'
            else channel end                                                 channel_grouped,
       contacts.*

from {{ ref('stg_hs_contacts_attributed_prep') }} as contacts
            left outer join {{ ref('seed_seo_page_groups') }} pg on pg.page = lower(contacts.hutk_analytics_first_page)