select con.created_at                                             as created_at,
           coalesce(md5(concat('company', con.hs_company_id)),md5(concat('contact', con.contact_id))) as client_id,
           users.user_id                                              as platform_user_id,
           nvl(con.firstname || ' ' || con.lastname, con.email)       as name,
           lower(coalesce(agg_orders.first_submitted_order_country_iso2, con.country_iso2))   as country_iso2,
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
           nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_campaign') }}, '') as utm_campaign,
           nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_content') }}, '') as utm_content,
           nullif(coalesce({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_source') }},{{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utmsource') }}), '') as utm_source,
           nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_term') }}, '') as utm_term,
           nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'utm_medium') }}, '') as utm_medium,
           nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'abt') }}, '') as test_name,
           nullif({{dbt_utils.get_url_parameter('hutk_analytics_first_url', 'abv') }}, '') as test_variant,
           con.first_page_seen_grouped,
           con.channel_drilldown1                                     as channel_drilldown_1,
           con.channel_drilldown2                                     as channel_drilldown_2,
           con.hubspot_owner_id                                       as hubspot_owner_id,
           con.bdr_owner_id                                           as bdr_owner_id,
           bdr.name                                                   as bdr_owner_name,
           con.bdr_campaign                                           as bdr_campaign,
           con.hubspot_owner_assigned_at,
           con.last_contacted_at,
           own.name                                                   as hubspot_owner_name,
           own.primary_team_name                                      as hubspot_owner_primary_team_name,
           con.is_strategic                                           as is_strategic,
           con.email_type                                             as email_type,
           con.lifecyclestage                                         as lifecyclestage,
           con.hs_lifecyclestage_lead_date                            as became_lead_at_contact,
           mql.mql_date                                               as became_mql_at_contact,
           mql.mql_type                                               as mql_type, 
           mql.mql_technology                                         as mql_technology,
           con.hs_lifecyclestage_salesqualifiedlead_date              as became_sql_at_contact,
           con.hs_lead_status                                         as hs_lead_status,
           con.contact_source                                         as contact_source,
           jobtitle                                                   as job_title,
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
           dc.sub_region,
           lower(dc.continent)                                        as continent,
           teams.team_created_at                                      as team_created_at,
           teams.team_id                                              as team_id,
           teams.team_name                                            as team_name,
           teams.invited_at                                           as team_invited_at,
           teams.invite_accepted_at                                   as team_invite_accepted_at,
           teams.invite_status                                        as team_invite_status,                                                            
           case when teams.team_name is not null and (team_invite_status = 'accepted' or team_invite_status is null)  
           then true else false end                                   as is_team_member,
           users.created_at                                           as platform_user_created_at
    from {{ ref('stg_hs_contacts_attributed') }} as con
             left join {{ ref('stg_contacts_mqls') }} as mql on  con.contact_id = mql.contact_id
             left join {{ ref('agg_orders_contacts') }} as agg_orders on con.contact_id = agg_orders.hubspot_contact_id
             left join {{ ref('prep_countries') }} dc on  lower(coalesce(agg_orders.first_submitted_order_country_iso2, con.country_iso2))  = lower(dc.alpha2_code)
             left join {{ ref('hubspot_owners') }} own
                             on own.owner_id::bigint = con.hubspot_owner_id::bigint
             left join {{ ref('hubspot_owners') }} bdr on bdr.owner_id = con.bdr_owner_id
             left join {{ ref('stg_contacts_teams') }} teams on teams.hubspot_contact_id = con.contact_id
             left join {{ ref('prep_users')}} users on users.hubspot_contact_id = con.contact_id and users.hubspot_contact_id is not null and rnk_desc_hubspot_contact_id = 1
