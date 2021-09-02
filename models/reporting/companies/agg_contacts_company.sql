-- This table is not materialized in the database

{{ 
    config(
        materialized='ephemeral'
    )
}}

select hubspot_company_id,
       count(hubspot_contact_id)              as number_of_contacts,
       max(inside_mql_number)                 as number_of_inside_mqls,
       max(inside_opportunity_number)         as number_of_inside_opportunities,
       max(inside_customer_number)            as number_of_inside_customers

from {{ ref('stg_contacts_companies') }}

group by 1