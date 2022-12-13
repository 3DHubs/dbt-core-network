  
  with union_contacts as (
  {{ dbt_utils.union_relations(
    relations=[source('data_lake', 'legacy_contacts'), ref('hubspot_contacts') ]
  )}}

  )
select  
       lifecyclestage,
       lead_source,
       lastname,
       jobtitle,
       ip_country_code,
       country,
       hubspot_user_token,
       hubspot_owner_id,
       hs_lifecyclestage_salesqualifiedlead_date,
       hs_lifecyclestage_marketingqualifiedlead_date,
       hs_lifecyclestage_lead_date,
       hs_lead_status,
       hs_analytics_source_data_2,
       hs_analytics_source_data_1,
       hs_analytics_source,
       hs_analytics_first_visit_timestamp,
       hs_analytics_first_url,
       firstname,
       email_type,
       email,
       hubspot_owner_assigned_date,
       createdate,
       contact_id,
       bdr_assigned,
       associatedcompanyid,
       strategic,
       bdr_campaign,
       notes_last_contacted,
       first_cart_uuid,
       is_legacy,
       rank() over (partition by email order by createdate desc) as rnk_desc_email 
from union_contacts