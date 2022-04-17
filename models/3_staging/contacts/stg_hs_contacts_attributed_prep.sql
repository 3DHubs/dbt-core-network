{{
       config(
              materialized='table',
              post_hook="analyze {{ this }}"
       )
}}

with hc as (
    select contact_id,
           email,
           firstname,
           lastname,
           hs_analytics_source,
           hubspot_user_token,
           hs_analytics_source_data_1,
           hs_analytics_source_data_2,
           hs_analytics_first_url,
           hs_analytics_first_visit_timestamp,
           ip_country_code,
           lower(coalesce(hc.ip_country_code, dc.alpha2_code)) as country_iso2,
           createdate as created_at,
           lifecyclestage,
           associatedcompanyid,
           hs_lifecyclestage_lead_date,
           hs_lifecyclestage_marketingqualifiedlead_date,
           hs_lifecyclestage_salesqualifiedlead_date,
           hubspot_owner_id,
           bdr_assigned,
           bdr_campaign,
           strategic as is_strategic,
           email_type,
           hubspot_owner_assigned_date as hubspot_owner_assigned_at, 
           notes_last_contacted as last_contacted_at,
           hs_lead_status,
           lead_source as contact_source, 
           jobtitle,
           is_legacy
    from {{ ref('stg_hs_contacts_union_legacy') }} hc
    left join {{ ref('prep_countries') }} dc on (lower(dc.name) = lower(hc.country) or lower(dc.alpha2_code) = lower(hc.country))
)
select hc.contact_id,
       hc.email,
       hc.firstname,
       hc.lastname,
       first_value(lower(hc.hs_analytics_source))
           over ( partition by nvl(nullif(hc.hubspot_user_token, ''), hc.contact_id::varchar)
               order by least(hc.hs_analytics_first_visit_timestamp::timestamp, hc.created_at) asc
               rows between unbounded preceding and unbounded following)                   as hutk_analytics_source,
           first_value(hc.hs_analytics_source_data_1)
           over ( partition by nvl(nullif(hc.hubspot_user_token, ''), hc.contact_id::varchar)
               order by least(hc.hs_analytics_first_visit_timestamp::timestamp, hc.created_at) asc
               rows between unbounded preceding and unbounded following)                   as hutk_analytics_source_data_1,
           first_value(hc.hs_analytics_source_data_2)
           over ( partition by nvl(nullif(hc.hubspot_user_token, ''), hc.contact_id::varchar)
               order by least(hc.hs_analytics_first_visit_timestamp::timestamp, hc.created_at) asc
               rows between unbounded preceding and unbounded following)                   as hutk_analytics_source_data_2,
           first_value(split_part(hc.hs_analytics_first_url, '#', 1))
           over ( partition by nvl(nullif(hc.hubspot_user_token, ''), hc.contact_id::varchar)
               order by least(hc.hs_analytics_first_visit_timestamp::timestamp, hc.created_at) asc
               rows between unbounded preceding and unbounded following)                   as hutk_analytics_first_url,
               case when len({{ dbt_utils.get_url_path(field='hutk_analytics_first_url') }})  < 2 then '/' else replace(('/' + {{ dbt_utils.get_url_path(field='hutk_analytics_first_url') }} + '/'),'//','/') end as hutk_analytics_first_page,
           first_value(hc.hs_analytics_first_visit_timestamp)
           over ( partition by nvl(nullif(hc.hubspot_user_token, ''), hc.contact_id::varchar)
               order by least(hc.hs_analytics_first_visit_timestamp::timestamp, hc.created_at) asc
               rows between unbounded preceding and unbounded following)                   as hutk_analytics_first_visit_timestamp,
       hc.ip_country_code,
       hc.created_at,
       case
           when hc.lifecyclestage is null then 'unknown'
           else hc.lifecyclestage end                                                                as lifecyclestage,
       hc.associatedcompanyid                                                                        as hs_company_id,
       hc.hs_lifecyclestage_lead_date,
       hc.hs_lifecyclestage_marketingqualifiedlead_date,
       hc.hs_lifecyclestage_salesqualifiedlead_date,
       hc.country_iso2,
       hc.hubspot_owner_id                                                                           as hubspot_owner_id,
       hc.bdr_assigned                                                                               as bdr_owner_id,
       hc.bdr_campaign,
       hc.is_strategic,
       hc.email_type,
       hc.hubspot_owner_assigned_at,
       hc.last_contacted_at,
       hc.hs_lead_status,
       hc.contact_source,
       hc.jobtitle,
       is_legacy,
       case
           when lower(hutk_analytics_source) ~ 'offline' and
                lower(hutk_analytics_source_data_1) ~ 'import' and
                hc.created_at > '2018-11-18' then 'outbound'
           when lower(hutk_analytics_source_data_1) = 'integration' and
                hutk_analytics_source_data_2 = '52073'
               then 'outbound'
           else 'inbound' end                                                                        as channel_type
from hc