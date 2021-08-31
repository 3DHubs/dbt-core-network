----------------------------------------------------------------
-- DEAL AGGREGATES
----------------------------------------------------------------

-- This table is built from the fact_orders table 
-- and later appended into the [type table name] table.


with complete_orders as (
  
-- This unions the fact_orders table together with the missing_orders table which is a static table
-- that contains deals from both Drupal and Hubspot that are not found in service supply (~9K).

  {{ dbt_utils.union_relations(
    relations=[ref('stg_fact_orders'), source('data_lake', 'legacy_orders')]
) }}

-- The DBT union relations package unions tables even when they have different widths and column orders

)

select order_uuid,

       -- Contact Fields
       case
           when hubspot_contact_id is not null then
           min(order_submitted_at) over (partition by hubspot_contact_id) end                                                          as became_opportunity_date_contact,
       case
           when hubspot_contact_id is not null then
           min(order_closed_at) over (partition by hubspot_contact_id) end                                                                   as became_customer_date_contact,
       case
           when order_is_closed is true and hubspot_contact_id is not null then rank()
                                                                          over (partition by hubspot_contact_id order by order_closed_at asc) end as closed_deal_number_contact,
       case
           when hubspot_contact_id is not null then round(
                       extract(minutes from (order_closed_at - lag(order_closed_at)
                                                               over (partition by hubspot_contact_id order by order_closed_at asc))) /
                       1440,
                       1) end                                                                                                                     as closed_deal_days_between_previous_deal_contact,
       coalesce(datediff('month', became_customer_date_contact, order_closed_at) = 0, false)                                                                       as is_new_customer_contact,

       -- Company Fields
       case
           when hubspot_company_id is not null
               then min(order_submitted_at) over (partition by hubspot_company_id) end                                                      as became_opportunity_date_company,
       case
           when hubspot_company_id is not null
               then min(order_closed_at) over (partition by hubspot_company_id) end                                                               as became_customer_date_company,
       case
           when order_is_closed is true and hubspot_company_id is not null then rank()
                                                                          over (partition by hubspot_company_id order by order_closed_at asc) end as closed_deal_number_company,
       case
           when hubspot_company_id is not null then round(
                       extract(minutes from (order_closed_at - lag(order_closed_at)
                                                               over (partition by hubspot_company_id order by order_closed_at asc))) /
                       1440,
                       1) end                                                                                                                     as closed_deal_days_between_previous_deal_company,
       coalesce(datediff('month', became_customer_date_company, order_closed_at) = 0, false)                                                                       as is_new_customer_company,
       case
           when hubspot_company_id is not null then min(case when bdr_owner_name is not null then order_closed_at end)
                                                    over (partition by hubspot_company_id) end                                                    as first_bdr_owner_date_company

from complete_orders as orders
