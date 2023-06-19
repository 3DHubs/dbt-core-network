select dcu.hubspot_contact_id,
       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'gclid') }}, 
              '')           as advertising_gclid,
       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'msclkid') }}, 
              '')           as advertising_msclkid,
       case
              when advertising_gclid is not null or (lower(nullif(
                     {{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_source')}},
                     '')) like '%adwords%') then 'adwords'--TODO: Requires update to Google Ads?
              when advertising_msclkid is not null or (lower(nullif(
                     {{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_source')}},
                     '')) like '%bing%') then 'bing'
              else lower(nullif(
                     {{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_source')}},
                     '')) end as advertising_source,
       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'hsa_cam') }}, 
              '')           as stg_hsa_cam,
       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'hsa_grp') }}, 
              '')           as stg_hsa_grp,
       nullif(split_part(split_part(replace(
              {{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'hsa_tgt') }},'%3A',':'),
                                   'kwd-', 2), ':', 1),
              '')::bigint   as stg_hsa_keyword_id,

       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_campaign') }}, 
              '')           as stg_utm_campaign,
       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_content') }}, 
              '')           as stg_utm_content,
       nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_term') }}, 
              '')           as stg_utm_term,
       hutk_analytics_first_url,
       hutk_analytics_first_visit_timestamp

from {{ ref('stg_dim_contacts') }} as dcu

where channel in ('paid_search', 'branded_paid_search')

