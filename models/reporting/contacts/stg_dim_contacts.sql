select con.createdate                                             as created_date,
           md5(concat('contact', con.contact_id))                     as client_id,
           nvl(con.firstname || ' ' || con.lastname, con.email)       as name,
           con.country_iso2,
           con.hs_company_id                                          as hubspot_company_id,
           con.contact_id                                             as hubspot_contact_id,
           nullif(trim(con.hutk_analytics_source), '')                as hutk_analytics_source,
           nullif(trim(con.hutk_analytics_source_data_1), '')         as hutk_analytics_source_data_1,
           nullif(trim(con.hutk_analytics_source_data_2), '')         as hutk_analytics_source_data_2,
           nullif(trim(con.hutk_analytics_first_url), '')             as hutk_analytics_first_url,
           nullif(trim(con.hutk_analytics_first_visit_timestamp), '') as hutk_analytics_first_visit_timestamp,
           nullif(trim(con.channel_type), '')                         as channel_type,
           nullif(trim(con.channel), '')                              as channel,
           con.channel_grouped,
           nullif(trim(con.first_page_seen), '')                      as first_page_seen,
           case
               when lower(split_part(first_page_seen, '/', 2)) in ('js', 's3') then 'en'
               when len(split_part(first_page_seen, '/', 2)) = 2 then
                   lower(split_part(first_page_seen, '/', 2))
               else 'en' end                                          as first_page_seen_language,
           nullif(query_to_json(con.first_page_seen_query), '')       as first_page_seen_query,
           con.first_page_seen_grouped,
           con.channel_drilldown1                                     as channel_drilldown_1,
           con.channel_drilldown2                                     as channel_drilldown_2,
           con.hubspot_owner_id                                       as hubspot_owner_id,
           con.bdr_owner_id                                           as bdr_owner_id,
           bdr.first_name || ' ' || bdr.last_name                     as bdr_owner_name,
           con.hubspot_owner_assigned_date                            as hubspot_owner_assigned_date,
           own.first_name || ' ' || own.last_name                     as hubspot_owner_name,
           own.primary_team_name                                      as hubspot_owner_primary_team_name,
           con.account_category                                       as contact_category,
           con.email_type                                             as email_type,
           con.lifecyclestage                                         as lifecyclestage,
           con.hs_lifecyclestage_lead_date                            as became_lead_at_contact,
           mql.mql_date                                               as became_mql_at_contact,
           con.hs_lifecyclestage_salesqualifiedlead_date              as became_sql_at_contact,
           con.hs_lead_status                                         as hs_lead_status,
           con.is_sales_qualified                                     as is_sales_qualified,
           con.contact_source                                         as contact_source,
           jobtitle                                                   as job_title,
           con.lead_score                                             as lead_score,
           case
               when lower(job_title) ~ 'college|faculty|fellow|graduate|lecturer|learning|student|teacher'
                   then 'University'
               when lower(job_title) ~
                    'chief|director|executive|manag|ceo|coo|c-level|owner|cto|cmo|principal|president|vp|founder|head|director|chairman|partner|supervisor'
                   then 'Manager'
               when lower(job_title) ~ 'purchas|buy|procur|order' then 'Procurement'
               when lower(job_title) ~ 'engin|mech|design|technic|product|research|develop|ingénieur'
                   then 'Engineer or Designer'
               when lower(job_title) is null then null
               else 'Other' end                                       as job_role,
           dc.name                                                    as country_name,
           dc.market,
           dc.region,
           lower(dc.continent)                                        as continent
    from {{ ref('stg_hs_contacts_attributed') }} as con
             left join {{ ref('stg_contacts_mqls') }} as mql on  con.contact_id = mql.contact_id
             left join {{ ref('countries') }} dc on lower(con.country_iso2) = lower(dc.alpha2_code)
             left join {{ source('data_lake', 'hubspot_owners') }} own
                             on own.is_current = true and own.owner_id::bigint = con.hubspot_owner_id::bigint
             left join {{ source('data_lake', 'hubspot_owners') }} bdr on bdr.is_current = true and bdr.owner_id = con.bdr_owner_id