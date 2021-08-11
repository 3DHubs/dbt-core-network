{{ 
    config(
        materialized='ephemeral'
    )
}}

select hs_company_id,
       count(hs_contact_id)                   as number_of_contacts,
       max(became_company_mql_number)         as number_of_company_mqls,
       max(became_company_opportunity_number) as number_of_company_opportunities,
       max(became_company_customer_number)    as number_of_company_customers,
       min(became_customer_date)              as became_customer_date,
       max(recent_closed_order_date)          as recent_closed_order_date,
       min(second_order_closed_at)            as second_order_closed_at
from {{ ref('dim_contacts') }}
group by 1