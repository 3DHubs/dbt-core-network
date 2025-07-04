with city_coordinates as (
select distinct country_id,
                lower(locality)      as city,
                max(lat)                  as city_lat,
                max(lon)                  as city_lon
from {{ ref('addresses') }}
where locality is not null and lat is not null
group by 1,2
),
companies_btyd as (
select distinct id as company_id,
    first_value(btyd.alive_probability)
           over (
               partition by id order by btyd.btyd_date desc rows between unbounded preceding and unbounded following) as alive_probability
from {{ source('int_analytics', 'btyd') }}
where date_trunc('week', snapshot_date) = (select date_trunc('week', max(snapshot_date)) from {{ source('int_analytics', 'btyd') }})
), fact_retention as(

    select *
    from {{ ref("fact_retention") }} as fr
    where fr.is_latest_data_point
), fact_retention_historic as(

    select fr.hubspot_company_id,
       max(fr.reactivated_at) as last_reactivated_at,
       max(fr.churned_at) as last_churned_at
    from {{ ref("fact_retention") }} as fr
    group by 1
)

select 
       -- Fields from HS Companies (Stitch)
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
       hc.true_outbound,
       hc.tier, --JG unclear what this tier means
       hc.is_qualified,
       hc.strategic as is_strategic,
       hc.ultra_strategic as is_priority_account,
       hc.ultimate_company_owner_role                                            as hubspot_ultimate_company_owner_role,
       hc.became_strategic_date,
       hc.hubspot_owner_assigned_date,
       hc.notes_last_updated_at                                                  as last_activity_at,
       hc.notes_last_contacted_at                                                as last_contacted_at,
       hc.is_outbound_handover,
       hc.outbound_handover_date,
       hc.us_sales_account_draft_date,
       hc.us_sales_account_draft_winning_bid,
       hc.us_sales_account_draft_total_bid,

       -- Source: Hubspot Owners
       own.name                                                                  as hubspot_owner_name,
       own.primary_team_name                                                     as hubspot_owner_primary_team_name,
       own_inside.name                                                           as hubspot_inside_owner_name,
       own_inside.primary_team_name                                              as hubspot_inside_owner_primary_team_name,
       own_handover.name                                                         as hubspot_handover_owner_name,
       own_nss.name                                                              as hubspot_network_sales_specialist_name,
       uco.name                                                                  as hubspot_ultimate_company_owner_name,

       -- Source: Location
       lower(dc.alpha2_code)                                                     as country_iso2,
       dc.name                                                                   as country_name,
       lower(dc.continent)                                                       as continent,
       dc.market,
       dc.region,
       dc.sub_region,
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
       case when acc.became_mql_at_company  >= '2022-01-01' then acc.mql_type end as mql_type, --Cart data only proper available since 2022, hence mql_type.
       acc.became_mql_at_company                                                 as became_mql_at, 
       acc.mql_technology                                                        as mql_technology,
       acc.sign_up_source,

       -- Aggregates from Orders
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
       agg_orders.number_of_closed_projects_company                              as number_of_closed_projects,
       agg_orders.closed_sales_usd_company                                       as closed_sales_usd,           -- New Field (1st Sept 2021)
       agg_orders.closed_sales_usd_new_customer_company                          as closed_sales_usd_new_customer,
       agg_orders.total_precalc_margin_usd_new_customer_company                  as precalc_margin_usd_new_customer,
       agg_orders.first_submitted_order_technology_company                       as first_submitted_order_technology,
       agg_orders.first_closed_order_technology_company                          as first_closed_order_technology,
       nvl(agg_orders.is_integration_company,false)                              as is_integration_company,

       -- Aggregates from Contacts
       acc.number_of_inside_mqls,
       acc.number_of_inside_opportunities,
       acc.number_of_inside_customers,
       case when acc.has_team = 1 then true else false end as has_team,

       -- Other Fields
       indm.industry_mapped::varchar                                             as industry_mapped,
       btyd.alive_probability,
       sct.potential_tier,

       -- Retention fields
       fr.churned as is_churned,
       fr.churned_at as churned_at,
       fr.reactivated as is_reactiveted,
       fr.reactivated_at as reactivated_at,
       frh.last_reactivated_at,
       frh.last_churned_at,
       rfm.recency,
       rfm.frequency,
       rfm.monetary,
       rfm.r_score,
       rfm.f_score,
       rfm.m_score,
       rfm.rfm_segment
       
       
from {{ ref('hubspot_companies') }} hc
    left join {{ ref('mapping_industry') }} as indm
on lower(hc.industry) = lower(indm.industry)
    left join {{ ref('stg_companies_dimensions') }} as adc on hc.hubspot_company_id = adc.hubspot_company_id
    left join {{ ref('agg_contacts_company') }} as acc on hc.hubspot_company_id = acc.hubspot_company_id
    left join {{ ref('agg_orders_companies') }} as agg_orders on hc.hubspot_company_id = agg_orders.hubspot_company_id
    left join {{ ref('agg_orders_companies_rfm') }} as rfm on hc.hubspot_company_id = rfm.hubspot_company_id
    left join {{ ref('prep_countries') }} as dc on lower(coalesce(hc.country,adc.country_iso2)) = lower(dc.alpha2_code) or lower(coalesce(hc.country,adc.country_iso2)) = lower(dc.name)
    left join city_coordinates as cc on cc.city = lower(hc.city) and cc.country_id = dc.country_id 
    left join {{ ref('hubspot_owners') }} as own on own.owner_id::bigint = hc.hubspot_owner_id::bigint
    left join {{ ref('hubspot_owners') }} as own_inside on own_inside.owner_id::bigint = hc.inside_sales_owner::bigint
    left join {{ ref('hubspot_owners') }} as own_handover on own_handover.owner_id = hc.handover_owner
    left join {{ ref('hubspot_owners') }} as own_nss on own_nss.owner_id = hc.network_sales_specialist_id
    left join {{ ref('hubspot_owners') }} as uco on uco.owner_id = hc.ultimate_company_owner_id
    left join companies_btyd as btyd on btyd.company_id = hc.hubspot_company_id
    left join {{ ref('stg_customer_tiering') }} as sct on hc.hubspot_company_id = sct.hubspot_company_id
    left join fact_retention as fr on hc.hubspot_company_id = fr.hubspot_company_id
    left join fact_retention_historic as frh on hc.hubspot_company_id = frh.hubspot_company_id
where hc.hubspot_company_id >= 1