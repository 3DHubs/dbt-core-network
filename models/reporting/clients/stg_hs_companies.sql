{{
       config(
              materialized='table',
              post_hook="analyze {{ this }}"
       )
}}

with company_agg as (select distinct hc.hs_company_id,
                                         first_value(hc.contact_id)
                                         over ( partition by hs_company_id order by least(hutk_analytics_first_visit_timestamp::timestamp, createdate) asc rows between unbounded preceding and unbounded following) as contact_id,
                                         first_value(hc.country_iso2)
                                         over ( -- selects the country from the oldest contact of the company that has a country available
                                             partition by hs_company_id order by (hc.country_iso2 is null)::int, createdate asc rows between unbounded preceding and unbounded following)                            as country_iso2,
                                         first_value(hc.jobtitle)
                                         over (
                                             partition by hs_company_id order by (hc.jobtitle::varchar is null), createdate asc rows between unbounded preceding and unbounded following)                            as jobtitle,
                                         first_value(hc.email_type)
                                         over (
                                             partition by hs_company_id order by (hc.jobtitle::varchar is null), createdate asc rows between unbounded preceding and unbounded following)                            as email_type,
                                         min(hs_lifecyclestage_lead_date) over (partition by hs_company_id)                                                                                                          as became_lead_date,
                                         min(hs_lifecyclestage_marketingqualifiedlead_date)
                                         over (partition by hs_company_id)                                                                                                                                           as became_mql_date,
                                         min(hs_lifecyclestage_salesqualifiedlead_date)
                                         over (partition by hs_company_id)                                                                                                                                           as became_sql_date,
                                         first_value(hc.contact_source)
                                         over ( partition by hs_company_id order by least(hutk_analytics_first_visit_timestamp::timestamp, createdate) asc rows between unbounded preceding and unbounded following) as contact_source
                         from {{ ref('stg_hs_contacts_attributed') }} hc
                         where hs_company_id is not null)
select hc.createdate                                                             as created_date,
        md5(concat('company', hc.company_id))                                     as client_id,
        'company'                                                                 as type,
        name,
        hc.numberofemployees::int                                                 as number_of_employees,
        hc.industry::varchar                                                      as industry,
        indm.industry_mapped::varchar                                             as industry_mapped,
        hca.country_iso2,
        hc.company_id                                                             as hs_company_id,
        null::bigint                                                              as hs_contact_id,
        nullif(trim(hca.hutk_analytics_source), '')                               as hutk_analytics_source,
        nullif(trim(hca.hutk_analytics_source_data_1), '')                        as hutk_analytics_source_data_1,
        nullif(trim(hca.hutk_analytics_source_data_2), '')                        as hutk_analytics_source_data_2,
        nullif(trim(hca.hutk_analytics_first_url), '')                            as hutk_analytics_first_url,
        nullif(trim(hca.hutk_analytics_first_visit_timestamp), '')                as hutk_analytics_first_visit_timestamp,
        nullif(trim(hca.channel_type), '')                                        as channel_type,
        nullif(trim(hca.channel), '')                                             as channel,
        hca.channel_grouped,
        nullif(trim(hca.first_page_seen), '')                                     as first_page_seen,
        hca.first_page_seen_grouped,
        nullif(query_to_json(hca.first_page_seen_query), '')                      as first_page_seen_query,
        hca.channel_drilldown1                                                    as channel_drilldown_1,
        hca.channel_drilldown2                                                    as channel_drilldown_2,
        attempted_to_contact_date_company                                         as attempted_to_contact_at,
        connected_date_company                                                    as connected_at,
        ca.became_lead_date                                                       as became_lead_date,
        ca.became_mql_date                                                        as became_mql_date,
        ca.became_sql_date                                                        as became_sql_date,
        null::timestamp                                                           as became_inside_lead_date,
        null::timestamp                                                           as became_inside_mql_date,
        null::timestamp                                                           as became_inside_sql_date,
        hc.hubspot_owner_id::bigint                                               as hubspot_owner_id,
        bdr_owner_id                                                              as bdr_owner_id,
        nullif(ae_assigned, '')::int                                              as ae_id,
        trunc(hubspot_owner_assigneddate)                                         as hubspot_owner_assigned_date,
        added_as_strategic                                                        as became_strategic_date,
        nullif(hc.account_category, '')                                           as account_category,
        nullif(trim(ca.email_type), '')                                           as email_type,
        added_as_ae                                                               as became_ae_account_date,
        hc.hs_lead_status                                                         as hs_lead_status,
        case
            when sales_qualified = 'bdr_approved' then true
            when sales_qualified = 'bdr_denied'
                then false end                                                    as is_sales_qualified,
        ca.contact_source                                                         as contact_source,
        hc.founded_year::int                                                      as founded_year,
        case when hc.total_money_raised is not null then 'yes' else 'unknown' end as is_funded,
        ca.jobtitle                                                               as job_title,
        hc.deactivated                                                            as is_deactivated,
        hc.deactivated_date                                                       as deactivated_date,
        hc.reactivated_opportunity                                                as is_reactivated_opportunity,
        hc.reactivated_opportunity_date                                           as reactivated_opportunity_date,
        hc.reactivated_customer                                                   as is_reactivated_customer,
        hc.reactivated_customer_date                                              as reactivated_customer_date,
        hc.company_lead_score::int                                                as lead_score,
        hc.tier::int                                                              as tier,
        hc.qualified                                                              as is_qualified

from {{ source('data_lake', 'hubspot_companies') }} hc
            left outer join company_agg ca on hc.company_id = ca.hs_company_id
            left outer join {{ ref('stg_hs_contacts_attributed') }} hca on ca.contact_id = hca.contact_id
            left join {{ ref('industries') }} indm on indm.industry = lower(hc.industry)

where hc.company_id >= 1