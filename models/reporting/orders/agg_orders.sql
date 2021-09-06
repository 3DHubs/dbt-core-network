----------------------------------------------------------------
-- DEAL AGGREGATES
----------------------------------------------------------------

-- This table is built from the stg_fact_orders table and later appended into the fact_orders table. 
-- This table is also the foundation for the contacts and companies aggregates so the fields created here
-- can be used either in fact_orders, dim_companies or dim_contacts.


with complete_orders as (

-- This unions the stg_fact_orders table together with the missing_orders table which is a static table
-- that contains deals from both Drupal and Hubspot that are not found in service supply (~9K).

    {{ dbt_utils.union_relations(
    relations=[ref('stg_fact_orders'), source('data_lake', 'legacy_orders')]
) }}

-- The DBT union relations package unions tables even when they have different widths and column orders

), agg_orders_prep as (

-- This stage is necessary because window functions cannot be nested

select  

    order_uuid,

-- CONTACT FIELDS

    -- Lifecycle
    case when hubspot_contact_id is not null then min(submitted_at) over (partition by hubspot_contact_id) end                                         as became_opportunity_at_contact,
    case when hubspot_contact_id is not null then min(closed_at) over (partition by hubspot_contact_id) end                                            as became_customer_at_contact,
    nth_value(case when is_closed then closed_at else null end, 2)
       over (partition by hubspot_contact_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)          as second_order_closed_at_contact,
    max(closed_at) over (partition by hubspot_contact_id)                                                                                              as recent_closed_order_at_contact,
    coalesce(datediff('month', became_opportunity_at_contact, closed_at) = 0, false)                                                                   as closed_order_is_from_new_customer_contact,
    
    -- Counts
    count(order_uuid) over (partition by hubspot_contact_id)                                                                                           as number_of_orders_contact,
    count(case when is_submitted then order_uuid end) over (partition by hubspot_contact_id)                                                           as number_of_submitted_orders_contact,
    count(case when is_closed then order_uuid end) over (partition by hubspot_contact_id)                                                              as number_of_closed_orders_contact,

    -- Financial Totals
    nullif(sum(closed_amount_usd) over (partition by hubspot_contact_id), 0)                                                                           as closed_sales_usd_contact,

    -- First Values
    first_value(technology_name) 
        over (partition by hubspot_contact_id order by submitted_at asc rows between unbounded preceding and unbounded following)                      as first_submitted_order_technology_contact,
    first_value(case when is_closed then technology_name end) 
        over (partition by hubspot_contact_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)         as first_closed_order_technology_contact,
    first_value(case when is_closed then process_name end)
        over (partition by hubspot_contact_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)         as first_closed_order_process_name_contact,
    first_value(destination_country_iso2)
        over ( partition by hubspot_contact_id order by closed_at asc rows between unbounded preceding and unbounded following)                        as first_submitted_order_country_iso2,    

    -- Rank Values
    case when is_closed is true and hubspot_contact_id is not null then rank() over (partition by hubspot_contact_id order by closed_at asc) end       as closed_order_number_contact,

    -- Other Date Fields
    lag(closed_at) over (partition by hubspot_contact_id order by closed_at)                                                                           as previous_closed_order_at_contact,
    case when hubspot_contact_id is not null then round(extract(minutes from (closed_at - lag(closed_at)
        over (partition by hubspot_contact_id order by closed_at asc))) / 1440, 1) end                                                                 as days_from_previous_closed_order_contact,

-- COMPANY FIELDS

    -- Lifecycle
    case when hubspot_company_id is not null then min(submitted_at) over (partition by hubspot_company_id) end                                         as became_opportunity_at_company,
    case when hubspot_company_id is not null then min(closed_at) over (partition by hubspot_company_id) end                                            as became_customer_at_company,
    nth_value(case when is_closed then closed_at else null end, 2)
       over (partition by hubspot_company_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)          as second_order_closed_at_company,
    max(closed_at) over (partition by hubspot_company_id)                                                                                              as recent_closed_order_at_company,
    coalesce(datediff('month', became_customer_at_company, closed_at) = 0, false)                                                                      as closed_order_is_from_new_customer_company,
    
    -- Counts (Orders)
    count(order_uuid) over (partition by hubspot_company_id)                                                                                           as number_of_orders_company,
    count(case when is_submitted then order_uuid end) over (partition by hubspot_company_id)                                                           as number_of_submitted_orders_company,
    count(case when is_closed then order_uuid end) over (partition by hubspot_company_id)                                                              as number_of_closed_orders_company,

    -- Financial Totals
    nullif(sum(order_closed_amount_usd) over (partition by hubspot_company_id), 0)                                                                     as closed_sales_usd_company,

    -- First Values
    first_value(technology_name) 
        over (partition by hubspot_company_id order by submitted_at asc rows between unbounded preceding and unbounded following)                      as first_submitted_order_technology_company,
    first_value(case when is_closed then technology_name end) 
        over (partition by hubspot_company_id order by is_closed desc, closed_at asc rows between unbounded preceding and unbounded following)         as first_closed_order_technology_company,

    -- Rank Values
    case when is_closed is true and hubspot_company_id is not null then rank() 
        over (partition by hubspot_company_id order by closed_at asc) end                                                                              as closed_order_number_company,
        
    -- Other Date Fields
    case when hubspot_company_id is not null then round(extract(minutes from (closed_at - lag(closed_at)
        over (partition by hubspot_company_id order by closed_at asc))) / 1440, 1) end                                                                 as days_from_previous_closed_order_company,
    case when hubspot_company_id is not null then min(case when bdr_owner_name is not null then closed_at end) 
        over (partition by hubspot_company_id) end                                                                                                     as first_bdr_owner_at_company

from complete_orders

)

select orders.order_uuid,
       orders.hubspot_contact_id,
       orders.hubspot_company_id,

    --- CONTACT BASED FIELDS ---
        -- Lifecycle
       prep.became_opportunity_at_contact,
       prep.became_customer_at_contact,
       prep.second_order_closed_at_contact,
       prep.recent_closed_order_at_contact,
       prep.closed_order_is_from_new_customer_contact,
       -- Counts
       prep.number_of_orders_contact,
       prep.number_of_submitted_orders_contact,
       prep.number_of_closed_orders_contact,
       -- Financial Totals
       prep.closed_sales_usd_contact,
       sum(case when is_closed and closed_order_is_from_new_customer_contact then closed_amount_usd end) 
            over ( partition by hubspot_contact_id)                                                                                              as closed_sales_usd_new_customer_contact,
       sum(case when is_closed and closed_order_is_from_new_customer_contact then (sourced_amount_usd - sourced_cost_usd) end) 
            over (partition by hubspot_contact_id)                                                                                               as total_precalc_margin_usd_new_customer_contact,
       -- First Values
       prep.first_submitted_order_technology_contact,
       prep.first_closed_order_technology_contact,
       prep.first_closed_order_process_name_contact,
       prep.first_submitted_order_country_iso2,
       -- Rank Values
       prep.closed_order_number_contact,
       -- Other Date Fields
       prep.previous_closed_order_at_contact,
       prep.days_from_previous_closed_order_contact,

    --- COMPANY BASED FIELDS ---
       -- Lifecycle
       prep.became_opportunity_at_company,
       prep.became_customer_at_company,
       prep.second_order_closed_at_company,
       prep.recent_closed_order_at_company,
       prep.closed_order_is_from_new_customer_company,
       -- Counts
       prep.number_of_orders_company,
       prep.number_of_submitted_orders_company,
       prep.number_of_closed_orders_company,
       -- Financial
       prep.closed_sales_usd_company,
       sum(case when is_closed and closed_order_is_from_new_customer_company then closed_amount_usd end) 
            over ( partition by hubspot_company_id)                                                                                              as closed_sales_usd_new_customer_company,
       sum(case when is_closed and closed_order_is_from_new_customer_company then (sourced_amount_usd - sourced_cost_usd) end) 
            over (partition by hubspot_company_id)                                                                                               as total_precalc_margin_usd_new_customer_company,
       -- First Values
       prep.first_submitted_order_technology_company,
       prep.first_closed_order_technology_company,
       -- Rank Values
       prep.closed_order_number_company,
       prep.days_from_previous_closed_order_company,
       prep.first_bdr_owner_at_company

from complete_orders as orders
left join agg_orders_prep as prep on orders.order_uuid = prep.order_uuid
