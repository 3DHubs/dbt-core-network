----------------------------------------------------------------
-- DEAL AGGREGATES
----------------------------------------------------------------

-- This table is built from the stg_fact_orders table and later appended into the fact_orders table. 
-- This table is also the foundation for the contacts and companies aggregates so the fields created here
-- can be used either in fact_orders, dim_companies or dim_contacts.

{{ config(
    tags=["multirefresh"]
) }}

with complete_orders as (

-- This unions the stg_fact_orders table together with the missing_orders table which is a static table
-- that contains deals from both Drupal and Hubspot that are not found in service supply (~9K).

    {{ dbt_utils.union_relations(
    relations=[ref('stg_fact_orders'), source('int_analytics', 'legacy_orders')]
) }}

-- The DBT union relations package unions tables even when they have different widths and column orders

), agg_orders_prep as (

-- This stage is necessary because window functions cannot be nested

select  

    order_uuid,
    project_uuid,

-- CONTACT FIELDS

    -- Lifecycle
    case when hubspot_contact_id is not null then min(created_at) over (partition by hubspot_contact_id) end                                           as became_created_at_contact,
    case when hubspot_contact_id is not null then min(submitted_at) over (partition by hubspot_contact_id) end                                         as became_opportunity_at_contact,
    case when hubspot_contact_id is not null then min(closed_at) over (partition by hubspot_contact_id) end                                            as became_customer_at_contact,
    max(created_at) over (partition by hubspot_contact_id)                                                                                             as recent_order_created_at_contact,
    nth_value(case when is_closed then closed_at else null end, 2)
       over (partition by hubspot_contact_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)          as second_order_closed_at_contact,
    max(closed_at) over (partition by hubspot_contact_id)                                                                                              as recent_closed_order_at_contact,
    coalesce(datediff('month', became_created_at_contact, created_at) = 0, false)                                                                      as created_order_is_from_new_contact,
    coalesce(datediff('month', became_customer_at_contact, closed_at) = 0, false)                                                                      as closed_order_is_from_new_customer_contact,
    
    -- Counts
    count(order_uuid) over (partition by hubspot_contact_id)                                                                                           as number_of_orders_contact,
    count(order_uuid) over (partition by platform_user_id)                                                                                             as number_of_carts_platform_user_id,
    count(case when is_submitted then order_uuid end) over (partition by hubspot_contact_id)                                                           as number_of_submitted_orders_contact,
    count(case when is_closed then order_uuid end) over (partition by hubspot_contact_id)                                                              as number_of_closed_orders_contact,

    -- Financial Totals
    nullif(sum(subtotal_closed_amount_usd) over (partition by hubspot_contact_id), 0)                                                                  as closed_sales_usd_contact,

    -- First Values
    first_value(technology_name) 
        over (partition by hubspot_contact_id order by submitted_at asc rows between unbounded preceding and unbounded following)                      as first_submitted_order_technology_contact,
    first_value(case when is_closed then technology_name end) 
        over (partition by hubspot_contact_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)         as first_closed_order_technology_contact,
    first_value(case when is_closed then process_name end)
        over (partition by hubspot_contact_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)         as first_closed_order_process_name_contact,
    first_value(destination_country_iso2)
        over ( partition by hubspot_contact_id order by closed_at asc rows between unbounded preceding and unbounded following)                        as first_submitted_order_country_iso2,
    first_value(integration_platform_type) 
        over (partition by hubspot_contact_id order by created_at asc rows between unbounded preceding and unbounded following)                        as first_integration_type_contact,
    first_value(is_integration_mql_contact) 
        over (partition by hubspot_contact_id order by (is_integration_mql_contact is null)::int, created_at asc rows between unbounded preceding and unbounded following)                        as first_is_integration_mql_contact,
    first_value(is_integration_tmp) 
        over (partition by hubspot_contact_id order by closed_at asc rows between unbounded preceding and unbounded following)                         as is_integration_contact,
      

    -- Rank Values
    case when is_closed is true and hubspot_contact_id is not null then rank() over (partition by hubspot_contact_id order by closed_at asc) end       as closed_order_number_contact,

    -- Other Date Fields
    lag(closed_at) over (partition by hubspot_contact_id order by closed_at)                                                                           as previous_closed_order_at_contact,
    round( date_diff('minutes',  case when hubspot_contact_id is not null then lag(closed_at)
        over (partition by hubspot_contact_id order by closed_at asc) end ,closed_at) *1.0/1440,1)                                                     as days_from_previous_closed_order_contact,

-- COMPANY FIELDS

    -- Lifecycle
    case when hubspot_company_id is not null then min(created_at) over (partition by hubspot_company_id) end                                           as became_created_at_company,
    case when hubspot_company_id is not null then min(submitted_at) over (partition by hubspot_company_id) end                                         as became_opportunity_at_company,
    case when hubspot_company_id is not null then min(closed_at) over (partition by hubspot_company_id) end                                            as became_customer_at_company,
    max(created_at) over (partition by hubspot_company_id)                                                                                             as recent_order_created_at_company,
    nth_value(case when is_closed then closed_at else null end, 2)
       over (partition by hubspot_company_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)          as second_order_closed_at_company,
    max(closed_at) over (partition by hubspot_company_id)                                                                                              as recent_closed_order_at_company,
    coalesce(datediff('month', became_created_at_company, created_at) = 0, false)                                                                      as created_order_is_from_new_company,
    coalesce(datediff('month', became_customer_at_company, closed_at) = 0, false)                                                                      as closed_order_is_from_new_customer_company,
    
    -- Counts (Orders)
    count(order_uuid) over (partition by hubspot_company_id)                                                                                           as number_of_orders_company,
    count(case when is_submitted then order_uuid end) over (partition by hubspot_company_id)                                                           as number_of_submitted_orders_company,
    count(case when is_closed then order_uuid end) over (partition by hubspot_company_id)                                                              as number_of_closed_orders_company,

    -- Financial Totals
    nullif(sum(subtotal_closed_amount_usd) over (partition by hubspot_company_id), 0)                                                                  as closed_sales_usd_company,

    -- First Values
    first_value(technology_name) 
        over (partition by hubspot_company_id order by submitted_at asc rows between unbounded preceding and unbounded following)                      as first_submitted_order_technology_company,
    first_value(case when is_closed then technology_name end) 
        over (partition by hubspot_company_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)         as first_closed_order_technology_company,
    first_value(is_integration_tmp) 
        over (partition by hubspot_company_id order by closed_at asc rows between unbounded preceding and unbounded following)                         as is_integration_company,
    first_value(is_integration_mql_contact) 
        over (partition by hubspot_company_id order by closed_at asc rows between unbounded preceding and unbounded following)                         as first_is_integration_mql_company,

    -- Rank Values
    case when is_closed is true and hubspot_company_id is not null then rank() 
        over (partition by hubspot_company_id order by closed_at asc) end                                                                              as closed_order_number_company,
    case when is_closed then dense_rank() over (partition by hubspot_company_id order by cast(closed_at as date) asc) else null end                    as closed_project_number_company,    
        
    -- Other Date Fields
    round( date_diff('minutes',  case when hubspot_company_id is not null then lag(closed_at)
        over (partition by hubspot_company_id order by closed_at asc) end ,closed_at) *1.0/1440,1)                                                     as days_from_previous_closed_order_company,
    case when hubspot_company_id is not null then min(case when bdr_owner_name is not null then closed_at end) 
        over (partition by hubspot_company_id) end                                                                                                     as first_bdr_owner_at_company
,
-- CLIENT FIELDS

    -- Lifecycle

    case when hubspot_contact_id is not null then min(created_at) over (partition by coalesce(hubspot_company_id,hubspot_contact_id)) end              as became_created_at_client,
    case when hubspot_contact_id is not null then min(submitted_at) over (partition by coalesce(hubspot_company_id,hubspot_contact_id)) end            as became_opportunity_at_client,
    case when hubspot_contact_id is not null then min(closed_at) over (partition by coalesce(hubspot_company_id,hubspot_contact_id)) end               as became_customer_at_client,
    coalesce(datediff('month', became_created_at_client, created_at) = 0, false)                                                                       as created_order_is_from_new_client,
    coalesce(datediff('month', became_customer_at_client, closed_at) = 0, false)                                                                       as closed_order_is_from_new_customer_client,

        -- Rank Values
    case when is_closed is true and hubspot_contact_id is not null then rank() over (partition by coalesce(hubspot_company_id,hubspot_contact_id) order by closed_at asc) end       as closed_order_number_client,


-- PROJECT FIELDS

    sum(subtotal_amount_usd) over (partition by project_uuid)                                                                                          as project_amount_usd,
    count(order_uuid) over (partition by project_uuid)                                                                                                 as project_order_count,
    case when closed_at = (min(closed_at) over (partition by project_uuid)) and sum(case when has_rfq then 1 else 0 end) over (partition by project_uuid) > 0 then true else false end as project_has_rfq

from complete_orders

), 
    --- Logic for new product KPIS 8 sept, measuring time between series of orders
serie_two as (
select orders.order_uuid,
       orders.hubspot_contact_id,
       orders.hubspot_company_id,
       orders.created_at,
       orders.closed_at,
       case when created_at > dateadd(day, 7, became_customer_at_contact) then created_at else null end                                                as serie_two_created_at_after_first_order_contact,
       min(serie_two_created_at_after_first_order_contact) over (partition by orders.hubspot_contact_id)                                               as serie_two_order_created_at_contact,
       case when closed_at > serie_two_order_created_at_contact then closed_at else null end                                                           as p_serie_two_order_closed_at_contact,
       case when created_at > dateadd(day, 7, became_customer_at_company) then created_at else null end                                                as serie_two_created_at_after_first_order_company,
       min(serie_two_created_at_after_first_order_company) over (partition by orders.hubspot_company_id)                                               as serie_two_order_created_at_company,
       case when closed_at > serie_two_order_created_at_company then closed_at else null end                                                           as p_serie_two_order_closed_at_company
from complete_orders as orders
left join agg_orders_prep as prep on orders.order_uuid = prep.order_uuid
),
serie_two_closed as (
select order_uuid,
       hubspot_contact_id,
       hubspot_company_id,
       created_at,
       closed_at,
       serie_two_order_created_at_contact,
       min(p_serie_two_order_closed_at_contact) over (partition by hubspot_contact_id)                                                                 as serie_two_order_closed_at_contact,
       case when created_at > dateadd(day, 7, serie_two_order_closed_at_contact) then created_at else null end                                         as serie_three_created_at_after_serie_two_first_order_contact,
       serie_two_order_created_at_company,
       min(p_serie_two_order_closed_at_company) over (partition by hubspot_company_id)                                                                 as serie_two_order_closed_at_company,
       case when created_at > dateadd(day, 7, serie_two_order_closed_at_company) then created_at else null end                                         as serie_three_created_at_after_serie_two_first_order_company
from serie_two
),
serie_three_created as (
select order_uuid,
       hubspot_contact_id,
       hubspot_company_id,
       created_at,
       closed_at,
       serie_two_order_created_at_contact,
       serie_two_order_closed_at_contact,
       min(serie_three_created_at_after_serie_two_first_order_contact) over (partition by hubspot_contact_id)                                          as serie_three_order_created_at_contact,
       case when closed_at > serie_three_order_created_at_contact then closed_at else null end                                                         as p_serie_three_order_closed_at_contact,
       serie_two_order_created_at_company,
       serie_two_order_closed_at_company,
       min(serie_three_created_at_after_serie_two_first_order_company) over (partition by hubspot_company_id)                                          as serie_three_order_created_at_company,
       case when closed_at > serie_three_order_created_at_company then closed_at else null end                                                         as p_serie_three_order_closed_at_company
from serie_two_closed
),
serie_three_closed as (
select order_uuid,
       hubspot_contact_id,
       hubspot_company_id,
       created_at,
       closed_at,
       serie_two_order_created_at_contact,
       serie_two_order_closed_at_contact,
       serie_three_order_created_at_contact,
       min(p_serie_three_order_closed_at_contact) over (partition by hubspot_contact_id)                                                                 as serie_three_order_closed_at_contact,
       serie_two_order_created_at_company,
       serie_two_order_closed_at_company,
       serie_three_order_created_at_company,
       min(p_serie_three_order_closed_at_company) over (partition by hubspot_company_id)                                                                 as serie_three_order_closed_at_company
from serie_three_created
)
select orders.order_uuid,
       orders.project_uuid,
       orders.hubspot_contact_id,
       orders.hubspot_company_id,
       orders.platform_user_id,

    --- CONTACT BASED FIELDS ---
        -- Lifecycle
       prep.became_opportunity_at_contact,
       prep.became_customer_at_contact,
       serie.serie_two_order_created_at_contact,
       serie.serie_two_order_closed_at_contact,
       serie.serie_three_order_created_at_contact,
       serie.serie_three_order_closed_at_contact,
       prep.recent_order_created_at_contact,
       prep.recent_closed_order_at_contact,
       prep.second_order_closed_at_contact,
       prep.created_order_is_from_new_contact,
       prep.closed_order_is_from_new_customer_contact,
       -- Counts
       case when prep.number_of_closed_orders_contact > 0 then null else number_of_carts_platform_user_id end as number_of_carts_without_closed_carts_platform_user_id,
       prep.number_of_orders_contact,
       prep.number_of_submitted_orders_contact,
       prep.number_of_closed_orders_contact,
       -- Financial Totals
       prep.closed_sales_usd_contact,
       sum(case when is_closed and closed_order_is_from_new_customer_contact then subtotal_closed_amount_usd end)
            over ( partition by orders.hubspot_contact_id)                                                                                              as closed_sales_usd_new_customer_contact,
       sum(case when is_closed and closed_order_is_from_new_customer_contact then (subtotal_sourced_amount_usd - subtotal_sourced_cost_usd) end)
            over (partition by orders.hubspot_contact_id)                                                                                               as total_precalc_margin_usd_new_customer_contact,
       sum(case when orders.closed_at < date_add('days',90, prep.became_customer_at_contact) then (subtotal_sourced_amount_usd - po_first_sourced_cost_usd) end)
        over (partition by orders.hubspot_contact_id)                                                                                               as total_precalc_margin_usd_contact_90d,
       sum(case when orders.closed_at < date_add('months',24, prep.became_customer_at_contact) then (subtotal_sourced_amount_usd - po_first_sourced_cost_usd) end)
        over (partition by orders.hubspot_contact_id)                                                                                               as total_precalc_margin_usd_contact_24m,
       -- First Values
       prep.first_submitted_order_technology_contact,
       prep.first_closed_order_technology_contact,
       prep.first_closed_order_process_name_contact,
       prep.first_submitted_order_country_iso2,
       prep.first_integration_type_contact,
       first_is_integration_mql_contact,
       case when is_integration_company or first_is_integration_mql_contact or first_is_integration_mql_company then true 
            when first_integration_type_contact is not null then true else prep.is_integration_contact end as is_integration_contact,
    --    case when prep.is_integration_contact = true  and is_integration_tmp = true then 'direct'
    --         when is_integration_tmp then 'indirect' 
    --         when prep.is_integration_contact = true then 'indirect'
    --         when prep.is_integration_company = true then 'indirect' end as integration_contact_is_closed_type,
       
       -- Rank Values
       prep.closed_order_number_contact,
       -- Other Date Fields
       prep.previous_closed_order_at_contact,
       prep.days_from_previous_closed_order_contact,

    
    --- COMPANY BASED FIELDS ---
       -- Lifecycle
       prep.became_opportunity_at_company,
       prep.became_customer_at_company,
       serie.serie_two_order_created_at_company,
       serie.serie_two_order_closed_at_company,
       serie.serie_three_order_created_at_company,
       serie.serie_three_order_closed_at_company,
       prep.recent_order_created_at_company,
       prep.second_order_closed_at_company,
       prep.recent_closed_order_at_company,
       prep.created_order_is_from_new_company,
       prep.closed_order_is_from_new_customer_company,
       -- Counts
       prep.number_of_orders_company,
       prep.number_of_submitted_orders_company,
       prep.number_of_closed_orders_company,
       -- Financial
       prep.closed_sales_usd_company,
       sum(case when is_closed and closed_order_is_from_new_customer_company then subtotal_closed_amount_usd end)
            over ( partition by orders.hubspot_company_id)                                                                                              as closed_sales_usd_new_customer_company,
       sum(case when is_closed and closed_order_is_from_new_customer_company then (subtotal_sourced_amount_usd - subtotal_sourced_cost_usd) end)
            over (partition by orders.hubspot_company_id)                                                                                               as total_precalc_margin_usd_new_customer_company,
       -- First Values
       prep.first_submitted_order_technology_company,
       prep.first_closed_order_technology_company,
       case when first_is_integration_mql_company then true else prep.is_integration_company end is_integration_company,
    --    case when is_integration_tmp = true then 'direct'
    --         when is_integration_tmp then 'indirect' 
    --         when prep.is_integration_company = true then 'indirect'
    --         when prep.is_integration_contact = true then 'indirect' end as integration_company_is_closed_type,
       -- Rank Values
       prep.closed_order_number_company,
       prep.closed_project_number_company,
       prep.days_from_previous_closed_order_company,
       prep.first_bdr_owner_at_company,
           --- CLIENT BASED FIELDS ---
       -- Lifecycle
       became_created_at_client,
       became_opportunity_at_client,
       became_customer_at_client,
       created_order_is_from_new_client,
       closed_order_is_from_new_customer_client,
       closed_order_number_client,

    ---PROJECT BASED FIELDS -- 

       project_amount_usd,
       project_order_count,
       first_value(project_has_rfq) over (partition by orders.project_uuid order by orders.closed_at asc rows between unbounded preceding and unbounded following)               as is_rfq_project


from complete_orders as orders
left join agg_orders_prep as prep on orders.order_uuid = prep.order_uuid
left join serie_three_closed as serie on orders.order_uuid = serie.order_uuid