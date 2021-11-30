with city_coordinates as (
select distinct country_id,
                lower(locality)      as city,
                max(lat)                  as city_lat,
                max(lon)                  as city_lon
from {{ ref('addresses') }}
where locality is not null and lat is not null
group by 1,2
)
select 
       -- Fields from HS Contacts (Stitch)
       hc.created_at,
       hc.name,
       hc.number_of_employees,
       hc.industry,
       hc.hubspot_company_id,
       hc.attempted_to_contact_at,
       hc.connected_at,
       hc.hubspot_owner_id,
       hc.hs_lead_status,
       hc.founded_year,
       case when hc.total_money_raised is not null then true else null end as is_funded,
       hc.is_deactivated,
       hc.deactivated_date,
       hc.is_reactivated_opportunity,
       hc.reactivated_opportunity_date,
       hc.is_reactivated_customer,
       hc.reactivated_customer_date,
       hc.lead_score,
       hc.tier,
       hc.is_qualified,
       hc.strategic as is_strategic,
       hc.became_strategic_date,
       hc.hubspot_owner_assigned_date,
       hc.notes_last_updated_at                                                  as last_activity_at,
       hc.notes_last_contacted_at                                                as last_contacted_at,

       -- Source: Hubspot Owners
       own.first_name || ' ' || own.last_name                                    as hubspot_owner_name,
       own.primary_team_name                                                     as hubspot_owner_primary_team_name,
       own_inside.first_name || ' ' || own.last_name                             as hubspot_inside_owner_name,
       own_inside.primary_team_name                                              as hubspot_inside_owner_primary_team_name,

       -- Source: Location
       lower(coalesce(hc.country,adc.country_iso2))                              as country_iso2,
       dc.name                                                                   as country_name,
       lower(dc.continent)                                                       as continent,
       dc.market,
       dc.region,
       hc.city,
       cc.city_lat                                                               as city_latitude,
       cc.city_lon                                                               as city_longitude,

       -- Derived from Contacts
       adc.channel_type,
       adc.channel,
       adc.channel_grouped,
       adc.channel_drilldown_1,
       adc.channel_drilldown_2,
       adc.first_page_seen_grouped,
       adc.advertising_gclid,
       adc.advertising_msclkid,
       adc.advertising_click_date,
       adc.advertising_click_device,
       adc.advertising_source,
       adc.advertising_account_id,
       adc.advertising_campaign_id,
       adc.advertising_adgroup_id,
       adc.advertising_keyword_id,
       adc.advertising_campaign_group,

       -- Aggregates from Orders
       acc.became_mql_at_company                                                 as became_mql_at, 
       agg_orders.became_opportunity_at_company                                  as became_opportunity_at,      -- New Field (1st Sept 2021)   as 
       agg_orders.became_customer_at_company                                     as became_customer_at,
       agg_orders.serie_two_order_created_at_company                             as serie_two_order_created_at,
       agg_orders.serie_two_order_closed_at_company                              as serie_two_order_closed_at,
       agg_orders.serie_three_order_created_at_company                           as serie_three_order_created_at,
       agg_orders.serie_three_order_closed_at_company                            as serie_three_order_closed_at,
       agg_orders.recent_order_created_at_company                                as recent_order_created_at,
       agg_orders.second_order_closed_at_company                                 as second_order_closed_at,
       agg_orders.recent_closed_order_at_company                                 as recent_closed_order_at,
       agg_orders.number_of_submitted_orders_company                             as number_of_submitted_orders, -- New Field (1st Sept 2021)
       agg_orders.number_of_closed_orders_company                                as number_of_closed_orders,    -- New Field (1st Sept 2021)
       agg_orders.closed_sales_usd_company                                       as closed_sales_usd,           -- New Field (1st Sept 2021)
       agg_orders.closed_sales_usd_new_customer_company                          as closed_sales_usd_new_customer,
       agg_orders.total_precalc_margin_usd_new_customer_company                  as precalc_margin_usd_new_customer,
       agg_orders.first_submitted_order_technology_company                       as first_submitted_order_technology,
       agg_orders.first_closed_order_technology_company                          as first_closed_order_technology,

       -- Aggregates from Contacts
       acc.number_of_inside_mqls,
       acc.number_of_inside_opportunities,
       acc.number_of_inside_customers,
       case when acc.has_team = 1 then true else false end as has_team,

       -- Other Fields
       indm.industry_mapped::varchar                                             as industry_mapped
       
from {{ source('data_lake', 'hubspot_companies_stitch') }} hc
    left join {{ ref('industry_mapping') }} as indm
on lower(hc.industry) = indm.industry
    left join {{ ref('stg_companies_dimensions') }} as adc on hc.hubspot_company_id = adc.hubspot_company_id
    left join {{ ref('agg_contacts_company') }} as acc on hc.hubspot_company_id = acc.hubspot_company_id
    left join {{ ref('agg_orders_companies') }} as agg_orders on hc.hubspot_company_id = agg_orders.hubspot_company_id
    left join {{ ref('countries') }} as dc on lower( coalesce(hc.country,adc.country_iso2)) = lower(dc.alpha2_code)
    left join city_coordinates as cc on cc.city = lower(hc.city) and cc.country_id = dc.country_id 
    left join {{ source('data_lake', 'hubspot_owners') }} as own on own.is_current = true and own.owner_id::bigint = hc.hubspot_owner_id::bigint
    left join {{ source('data_lake', 'hubspot_owners') }} as own_inside on own_inside.is_current = true and own_inside.owner_id::bigint = hc.inside_sales_owner::bigint
where hc.hubspot_company_id >= 1