select hc.createdate                                                             as created_at,
       hc.name,
       hc.numberofemployees::int                                                 as number_of_employees,
       hc.industry::varchar                                                      as industry,
       indm.industry_mapped::varchar                                             as industry_mapped,
       hc.company_id                                                             as hubspot_company_id,
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
       hc.company_lead_score::int                                                as lead_score,
       hc.tier::int                                                              as tier,
       hc.qualified                                                              as is_qualified,
       -- Company Dimensions (Defined from Contacts)
       adc.country_iso2,
       dc.name                                                                   as country_name,
       lower(dc.continent)                                                       as continent,
       dc.market,
       dc.region,
       adc.channel_grouped,
       -- Aggregates from Orders
       agg_orders.became_opportunity_at_company                                 as became_opportunity_at, -- New Field (1st Sept 2021)   as 
       agg_orders.became_customer_at_company                                    as became_customer_at,   
       agg_orders.serie_two_order_created_at_company                            as serie_two_order_created_at,
       agg_orders.serie_two_order_closed_at_company                             as serie_two_order_closed_at,
       agg_orders.serie_three_order_created_at_company                          as serie_three_order_created_at,
       agg_orders.serie_three_order_closed_at_company                           as serie_three_order_closed_at,    
       agg_orders.recent_order_created_at_company                               as recent_order_created_at,
       agg_orders.second_order_closed_at_company                                as second_order_closed_at,     
       agg_orders.recent_closed_order_at_company                                as recent_closed_order_at,
       agg_orders.number_of_submitted_orders_company                            as number_of_submitted_orders, -- New Field (1st Sept 2021)
       agg_orders.number_of_closed_orders_company                               as number_of_closed_orders, -- New Field (1st Sept 2021)
       agg_orders.closed_sales_usd_company                                      as closed_sales_usd, -- New Field (1st Sept 2021)
       agg_orders.closed_sales_usd_new_customer_company                         as closed_sales_usd_new_customer, 
       agg_orders.total_precalc_margin_usd_new_customer_company                 as precalc_margin_usd_new_customer,
       agg_orders.first_submitted_order_technology_company                      as first_submitted_order_technology,
       agg_orders.first_closed_order_technology_company                         as first_closed_order_technology,
       -- Aggregates from Contacts
       acc.number_of_inside_mqls,
       acc.number_of_inside_opportunities,
       acc.number_of_inside_customers
from {{ source('data_lake', 'hubspot_companies') }} hc
    left join {{ ref('industry_mapping') }} as indm on lower(hc.industry) = indm.industry 
    left join {{ ref('stg_companies_dimensions') }} as adc on hc.company_id = adc.hubspot_company_id
    left join {{ ref('agg_contacts_company') }}     as acc on hc.company_id = acc.hubspot_company_id
    left join {{ ref('agg_orders_companies') }}     as agg_orders on hc.company_id = agg_orders.hubspot_company_id
    left join {{ ref('countries') }}                as dc on lower(adc.country_iso2) = lower(dc.alpha2_code)
    left join {{ source('data_lake', 'hubspot_owners') }} as own on own.is_current = true and own.owner_id::bigint = hc.hubspot_owner_id::bigint
    left join {{ source('data_lake', 'hubspot_owners') }} as ae on ae.is_current = true and ae.owner_id = hc.ae_assigned
where hc.company_id >= 1
