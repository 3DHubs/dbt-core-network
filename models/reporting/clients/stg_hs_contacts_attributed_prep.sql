{{
       config(
              materialized='table',
              post_hook="analyze {{ this }}"
       )
}}

-- This is an ephemeral model in preparation for `stg_hs_contacts_attributed.sql`.
with hc as (
    select hubspot_contacts.contact_id,
           hubspot_contacts.email,
           hubspot_contacts.firstname,
           hubspot_contacts.lastname,
           lower(hubspot_contacts.hs_analytics_source)                                       as hs_analytics_source,
           nullif(hubspot_contacts.hubspot_user_token, '')                                   as hubspot_user_token,
           nvl(hubspot_contacts.hubspot_user_token, hubspot_contacts.contact_id::varchar)                     as hs_user_uuid,
           least(hubspot_contacts.hs_analytics_first_visit_timestamp::timestamp, hubspot_contacts.createdate) as earliest_timestamp,
           lower(hubspot_contacts.hs_analytics_source_data_1)                                as hs_analytics_source_data_1,
           lower(hubspot_contacts.hs_analytics_source_data_2)                                as hs_analytics_source_data_2,
           hubspot_contacts.hs_analytics_first_url,
           hubspot_contacts.hs_analytics_first_visit_timestamp,
           coalesce(hubspot_contacts.ip_country_code, ac2.alpha2_code, country_names.alpha2_code) as ip_country_code,
           hubspot_contacts.createdate,
           nullif(hubspot_contacts.lifecyclestage, '')                                       as lifecyclestage,
           hubspot_contacts.associatedcompanyid,
           hubspot_contacts.hs_lifecyclestage_lead_date,
           hubspot_contacts.hs_lifecyclestage_marketingqualifiedlead_date,
           hubspot_contacts.hs_lifecyclestage_salesqualifiedlead_date,
           hubspot_contacts.hubspot_owner_id,
           hubspot_contacts.bdr_assigned,
           nullif(hubspot_contacts.account_category, '')                                     as account_category,
           nullif(hubspot_contacts.emailtype, '')                                            as email_type,
           trunc(hubspot_contacts.hubspot_owner_assigneddate)                                as
                                                                               hubspot_owner_assigned_date,
           nullif(hubspot_contacts.hs_lead_status, '')                                       as hs_lead_status,
           hubspot_contacts.bdr_qualification,
           nullif(hubspot_contacts.lead_source, '')                                          as contact_source,
           hubspot_contacts.jobtitle,
           hubspot_contacts.hubspotscore::int                                                as hubspotscore
    
    from {{ source('data_lake', 'hubspot_contacts') }}
    left outer join {{ ref('countries') }} as ac2
        on case when len(hubspot_contacts.country) = 2 then lower(hubspot_contacts.country) end = lower(ac2.alpha2_code)
    left outer join {{ ref('countries') }} as country_names
        on case when len(hubspot_contacts.country) >= 3 then lower(hubspot_contacts.country) end = lower(country_names."name")
)
select hc.contact_id,
       hc.email,
       hc.firstname,
       hc.lastname,
       first_value(hs_analytics_source)
       over ( partition by hs_user_uuid
           order by earliest_timestamp asc rows between unbounded preceding and unbounded following) as hutk_analytics_source,
       first_value(hs_analytics_source_data_1)
       over ( partition by hs_user_uuid
           order by earliest_timestamp asc rows between unbounded preceding and unbounded following) as hutk_analytics_source_data_1,
       first_value(hs_analytics_source_data_2)
       over ( partition by hs_user_uuid
           order by earliest_timestamp asc rows between unbounded preceding and unbounded following) as hutk_analytics_source_data_2,
       first_value(hs_analytics_first_url)
       over ( partition by hs_user_uuid
           order by earliest_timestamp asc rows between unbounded preceding and unbounded following) as hutk_analytics_first_url,
       urlsplit(hutk_analytics_first_url, 'path')                                                    as hutk_analytics_first_page,
       first_value(hs_analytics_first_visit_timestamp)
       over ( partition by hs_user_uuid
           order by earliest_timestamp asc rows between unbounded preceding and unbounded following) as hutk_analytics_first_visit_timestamp,
       nullif(hc.ip_country_code, '')                                                                as ip_country_code,
       hc.createdate,
       case
           when hc.lifecyclestage is null then 'unknown'
           else hc.lifecyclestage end                                                                as lifecyclestage,
       hc.associatedcompanyid                                                                        as hs_company_id,
       hc.hs_lifecyclestage_lead_date,
       hc.hs_lifecyclestage_marketingqualifiedlead_date,
       hc.hs_lifecyclestage_salesqualifiedlead_date,
       hc.ip_country_code                                                                            as country_iso2,
       hc.hubspot_owner_id                                                                           as hubspot_owner_id,
       hc.bdr_assigned                                                                               as bdr_owner_id,
       hc.account_category,
       hc.email_type,
       hc.hubspot_owner_assigned_date,
       hc.hs_lead_status,
       case
           when hc.bdr_qualification = 'bdr_approved' then true
           when hc.bdr_qualification = 'bdr_denied'
               then false end                                                                        as is_sales_qualified,
       hc.contact_source,
       hc.jobtitle,
       case when hc.createdate >= '2020-07-01' then hc.hubspotscore end                              as lead_score,
       case
           when hutk_analytics_source ~ 'offline' and
                hutk_analytics_source_data_1 ~ 'import' and
                hc.createdate > '2018-11-18' then 'outbound'
           when hutk_analytics_source_data_1 = 'integration' and
                hutk_analytics_source_data_2 = '52073'
               then 'outbound'
           else 'inbound' end                                                                        as channel_type
from hc