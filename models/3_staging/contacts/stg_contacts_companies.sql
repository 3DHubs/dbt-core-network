-- This is a model that "aggregates" values at the contact level into the company level, 
-- this is done through window functions and hence not really an aggregation, this is later aggregated
-- in another model to be used in dim_companies. Last update: 1 Sept 2021.

select contacts.hubspot_contact_id,
       contacts.hubspot_company_id,
       case
           when contacts.became_mql_at_contact is not null and contacts.hubspot_company_id is not null
               then rank() over (partition by contacts.hubspot_company_id order by contacts.became_mql_at_contact, contacts.created_at asc)
           when contacts.hubspot_company_id is null
               then 1
           else null end as inside_mql_number,
       contacts.became_mql_at_contact,
       contacts.mql_technology,
       contacts.mql_type,
       case
           when agg_orders.became_opportunity_at_contact is not null and contacts.hubspot_company_id is not null
               then rank() over (partition by contacts.hubspot_company_id order by agg_orders.became_opportunity_at_contact asc)
           when contacts.hubspot_company_id is null
               then 1
           else null end as inside_opportunity_number,
       case
           when agg_orders.became_customer_at_contact is not null and contacts.hubspot_company_id is not null
               then rank() over (partition by contacts.hubspot_company_id order by agg_orders.became_customer_at_contact asc)
           when contacts.hubspot_company_id is null
               then 1
           else null end as inside_customer_number,
       contacts.is_team_member
from {{ ref('stg_dim_contacts') }} as contacts
            left join {{ ref('agg_orders_contacts') }} as agg_orders on  contacts.hubspot_contact_id = agg_orders.hubspot_contact_id
