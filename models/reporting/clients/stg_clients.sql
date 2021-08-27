{{
       config(
              materialized='table'
       )
}}

with dim_clients_union as (select * from {{ ref('stg_hs_companies') }} union all select * from {{ ref('stg_hs_contacts') }})
    select dcu.created_date,
           dcu.client_id,
           dcu.type,
           dcu.name,
           dcu.number_of_employees,
           dcu.industry,
           dcu.industry_mapped,
           dcu.founded_year,
           dcu.is_funded,
           dcu.country_iso2,
           dcu.hs_company_id,
           dcu.hs_contact_id,
           dcu.hutk_analytics_source,
           dcu.hutk_analytics_source_data_1,
           dcu.hutk_analytics_source_data_2,
           dcu.hutk_analytics_first_url,
           dcu.hutk_analytics_first_visit_timestamp,
           dcu.channel_type,
           dcu.channel,
           dcu.channel_grouped,
           dcu.first_page_seen,
           dcu.first_page_seen_grouped,
           dcu.first_page_seen_query,
           nullif(json_extract_path_text(dcu.first_page_seen_query, 'utm_campaign'), '') as utm_campaign,
           nullif(json_extract_path_text(dcu.first_page_seen_query, 'utm_content'), '')  as utm_content,
           dcu.channel_drilldown_1,
           dcu.channel_drilldown_2,
           dcu.attempted_to_contact_at,
           dcu.connected_at,
           dcu.became_lead_date,
           dcu.became_mql_date,
           dcu.became_sql_date,
           dcu.became_inside_lead_date,
           dcu.became_inside_mql_date,
           dcu.became_inside_sql_date,
           dcu.hubspot_owner_id,
           dcu.became_ae_account_date,
           own.first_name || ' ' || own.last_name                                        as hubspot_owner_name,
           own.primary_team_name                                                         as hubspot_owner_primary_team_name,
           dcu.bdr_owner_id,
           bdr.first_name || ' ' || bdr.last_name                                        as bdr_owner_name,
           dcu.ae_id,
           ae.first_name || ' ' || ae.last_name                                          as ae_name,
           dcu.hubspot_owner_assigned_date,
           dcu.became_strategic_date,
           dcu.account_category,
           dcu.email_type,
           dcu.contact_source,
           dc.name                                                                       as country_name,
           dc.market,
           dc.region,
           dc.continent,
           dcu.hs_lead_status,
           dcu.is_sales_qualified,
           case
               when lower(dcu.job_title) ~ 'college|faculty|fellow|graduate|lecturer|learning|student|teacher'
                   then 'University'
               when lower(dcu.job_title) ~
                    'chief|director|executive|manag|ceo|coo|c-level|owner|cto|cmo|principal|president|vp|founder|head|director|chairman|partner|supervisor'
                   then 'Manager'
               when lower(dcu.job_title) ~ 'purchas|buy|procur|order' then 'Procurement'
               when lower(dcu.job_title) ~ 'engin|mech|design|technic|product|research|develop|ingénieur'
                   then 'Engineer or Designer'
               when lower(dcu.job_title) is null then null
               else 'Other' end                                                          as job_role,
           dcu.is_deactivated,
           dcu.deactivated_date,
           dcu.is_reactivated_opportunity,
           dcu.reactivated_opportunity_date,
           dcu.is_reactivated_customer,
           dcu.reactivated_customer_date,
           dcu.lead_score,
           dcu.tier,
           dcu.is_qualified

    from dim_clients_union dcu
             left outer join {{ ref('countries') }} dc on dcu.country_iso2 = lower(dc.alpha2_code)
             left outer join {{ source('data_lake', 'hubspot_owners') }} as own
                             on own.is_current = true and own.owner_id::bigint = dcu.hubspot_owner_id::bigint
             left outer join {{ source('data_lake', 'hubspot_owners') }} as bdr on bdr.is_current = true and bdr.owner_id = dcu.bdr_owner_id
             left outer join {{ source('data_lake', 'hubspot_owners') }} as ae on ae.is_current = true and ae.owner_id = dcu.ae_id