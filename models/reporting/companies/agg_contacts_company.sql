-- This table is not materialized in the database

{{ 
    config(
        materialized='ephemeral'
    )
}}

with prep_mql_technology as (
select
    hubspot_company_id, 
    first_value(mql_technology)
           over ( partition by hubspot_company_id order by (mql_technology is null)::int, became_mql_at_contact asc rows between unbounded preceding and unbounded following) as mql_technology
    from {{ ref('stg_contacts_companies') }}
)

select s.hubspot_company_id,
       mql.mql_technology,
       count(hubspot_contact_id)              as number_of_contacts,
       max(inside_mql_number)                 as number_of_inside_mqls,
       min(became_mql_at_contact)             as became_mql_at_company,
       max(inside_opportunity_number)         as number_of_inside_opportunities,
       max(inside_customer_number)            as number_of_inside_customers,
       max(is_team_member::int)               as has_team

from {{ ref('stg_contacts_companies') }} s
    left join prep_mql_technology mql on mql.hubspot_company_id = s.hubspot_company_id

group by 1,2