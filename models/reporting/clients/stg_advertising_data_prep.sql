select dcu.client_id,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'gclid'),
               '')           as advertising_gclid,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'msclkid'),
               '')           as advertising_msclkid,
       case
           when advertising_gclid is not null or (lower(nullif(
                   json_extract_path_text(dcu.first_page_seen_query, 'utm_source'),
                   '')) like '%adwords%') then 'adwords' --TODO: Requires update to Google Ads?
           when advertising_msclkid is not null or (lower(nullif(
                   json_extract_path_text(dcu.first_page_seen_query, 'utm_source'),
                   '')) like '%bing%') then 'bing'
           else lower(nullif(
                   json_extract_path_text(dcu.first_page_seen_query, 'utm_source'),
                   '')) end as advertising_source,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'hsa_cam'),
               '')::bigint   as stg_hsa_cam,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'hsa_grp'),
               '')::bigint   as stg_hsa_grp,
       nullif(split_part(split_part(
                                   json_extract_path_text(dcu.first_page_seen_query, 'hsa_tgt'),
                                   'kwd-', 2), ':', 1),
               '')::bigint   as stg_hsa_keyword_id,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'utm_campaign'),
               '')           as stg_utm_campaign,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'utm_content'),
               '')           as stg_utm_content,
       nullif(json_extract_path_text(dcu.first_page_seen_query, 'utm_term'),
               '')           as stg_utm_term,
       first_page_seen_query,
       hutk_analytics_first_visit_timestamp
from {{ ref('stg_clients') }} dcu
where channel in ('paid_search', 'branded_paid_search')