{{
       config(
              materialized='table',
              post_hook="analyze {{ this }}"
       )
}}

select con.createdate                                             as created_date,
           md5(concat('contact', con.contact_id))                     as client_id,
           'contact'                                                  as type,
           nvl(firstname || ' ' || lastname, email)                   as name,
           null::int                                                  as number_of_employees,
           null::varchar                                              as industry,
           null::varchar                                              as industry_mapped,
           con.country_iso2,
           con.hs_company_id,
           con.contact_id                                             as hs_contact_id,
           nullif(trim(con.hutk_analytics_source), '')                as hutk_analytics_source,
           nullif(trim(con.hutk_analytics_source_data_1), '')         as hutk_analytics_source_data_1,
           nullif(trim(con.hutk_analytics_source_data_2), '')         as hutk_analytics_source_data_2,
           nullif(trim(con.hutk_analytics_first_url), '')             as hutk_analytics_first_url,
           nullif(trim(con.hutk_analytics_first_visit_timestamp), '') as hutk_analytics_first_visit_timestamp,
           nullif(trim(con.channel_type), '')                         as channel_type,
           nullif(trim(con.channel), '')                              as channel,
           con.channel_grouped,
           nullif(trim(con.first_page_seen), '')                      as first_page_seen,
           con.first_page_seen_grouped,
           nullif(query_to_json(con.first_page_seen_query), '')       as first_page_seen_query,
           con.channel_drilldown1                                     as channel_drilldown_1,
           con.channel_drilldown2                                     as channel_drilldown_2,
           attempted_to_contact_at                                    as attempted_to_contact_at,
           connected_at                                               as connected_at,
           case
               when con.hs_company_id is null then con.hs_lifecyclestage_lead_date
               else null end                                          as became_lead_date,
           case
               when con.hs_company_id is null then con.hs_lifecyclestage_marketingqualifiedlead_date
               else null end                                          as became_mql_date,
           case
               when con.hs_company_id is null then con.hs_lifecyclestage_salesqualifiedlead_date
               else null end                                          as became_sql_date,
           case
               when con.hs_lifecyclestage_lead_date > com.became_lead_date then con.hs_lifecyclestage_lead_date
               else null end                                          as became_inside_lead_date,
           case
               when con.hs_lifecyclestage_marketingqualifiedlead_date > com.became_mql_date
                   then con.hs_lifecyclestage_marketingqualifiedlead_date
               else null end                                          as became_inside_mql_date,
           case
               when con.hs_lifecyclestage_salesqualifiedlead_date > com.became_sql_date
                   then con.hs_lifecyclestage_salesqualifiedlead_date
               else null end                                          as became_inside_sql_date,
           con.hubspot_owner_id                                       as hubspot_owner_id,
           con.bdr_owner_id                                           as bdr_owner_id,
           null::int                                                  as ae_id,
           con.hubspot_owner_assigned_date                            as hubspot_owner_assigned_date,
           null::timestamp                                            as became_strategic_date,
           con.account_category                                       as account_category,
           con.email_type                                             as email_type,
           null::timestamp                                            as became_ae_account_date,
           con.hs_lead_status                                         as hs_lead_status,
           con.is_sales_qualified                                     as is_sales_qualified,
           com.contact_source                                         as contact_source,
           null::int                                                  as founded_year,
           null::varchar                                              as is_funded,
           jobtitle                                                   as job_title,
           null::boolean                                              as is_deactivated,
           null::timestamp                                            as deactivated_date,
           null::boolean                                              as is_reactivated_opportunity,
           null::timestamp                                            as reactivated_opportunity_date,
           null::boolean                                              as is_reactivated_customer,
           null::timestamp                                            as reactivated_customer_date,
           con.lead_score                                             as lead_score,
           null::int                                                  as tier,
           null::boolean                                              as is_qualified
    from {{ ref('stg_hs_contacts_attributed') }} as con
             left outer join {{ ref('stg_hs_companies') }} com on con.hs_company_id = com.hs_company_id