select hc.createdate                                                             as created_date,
           hc.name,
           hc.numberofemployees::int                                                 as number_of_employees,
           hc.industry::varchar                                                      as industry,
           indm.industry_mapped::varchar                                             as industry_mapped,
           hc.company_id                                                             as hs_company_id,
           hc.attempted_to_contact_date_company                                      as attempted_to_contact_at,
           hc.connected_date_company                                                 as connected_at,
           hc.hubspot_owner_id::bigint                                               as hubspot_owner_id,
           own.first_name || ' ' || own.last_name                                    as hubspot_owner_name,
           own.primary_team_name                                                     as hubspot_owner_primary_team_name,
           nullif(hc.ae_assigned, '')::int                                           as ae_id,
           ae.first_name || ' ' || ae.last_name                                      as ae_name,
           trunc(hc.hubspot_owner_assigneddate)                                      as hubspot_owner_assigned_date,
           hc.added_as_strategic                                                     as became_strategic_date,
           nullif(hc.account_category, '')                                           as account_category,
           hc.added_as_ae                                                            as became_ae_account_date,
           ac.became_customer_date                                                   as became_customer_date,
           hc.hs_lead_status                                                         as hs_lead_status,
           case
               when hc.sales_qualified = 'bdr_approved' then true
               when hc.sales_qualified = 'bdr_denied'
                   then false end                                                    as is_sales_qualified,
           hc.founded_year::int                                                      as founded_year,
           case when hc.total_money_raised is not null then 'yes' else 'unknown' end as is_funded,
           hc.deactivated                                                            as is_deactivated,
           hc.deactivated_date                                                       as deactivated_date,
           hc.reactivated_opportunity                                                as is_reactivated_opportunity,
           hc.reactivated_opportunity_date                                           as reactivated_opportunity_date,
           hc.reactivated_customer                                                   as is_reactivated_customer,
           hc.reactivated_customer_date                                              as reactivated_customer_date,
           ac.recent_closed_order_date                                               as recent_closed_order_date,
           ac.second_order_closed_at                                                 as second_closed_order_date,
           hc.company_lead_score::int                                                as lead_score,
           hc.tier::int                                                              as tier,
           hc.qualified                                                              as is_qualified,
           adc.country_iso2,
           adc.continent,
           adc.market,
           adc.region,
           adc.channel_grouped,
           adc.first_order_technology,
           adc.first_quote_technology,
           dc.new_customer_closed_sales_usd,
           dc.new_customer_precalc_margin_usd,
           ac.number_of_company_mqls,
           ac.number_of_company_opportunities,
           ac.number_of_company_customers

from {{ source('data_lake', 'hubspot_companies') }} hc
            left outer join {{ ref('stg_companies_agg') }} as ac on ac.hs_company_id = hc.company_id
            left outer join {{ ref('stg_companies_dimensions') }} as adc on adc.hs_company_id = hc.company_id
            left outer join {{ ref('industry_mapping') }} as indm on indm.industry = lower(hc.industry)
            left outer join {{ source('data_lake', 'hubspot_owners') }} as own
                            on own.is_current = true and own.owner_id::bigint = hc.hubspot_owner_id::bigint
            left outer join {{ source('data_lake', 'hubspot_owners') }} as ae on ae.is_current = true and ae.owner_id = hc.ae_assigned
            left outer join {{ ref('stg_companies_deals') }} as dc on dc.hubspot_company_id = hc.company_id

where hc.company_id >= 1