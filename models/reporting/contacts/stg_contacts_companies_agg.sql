select hubspot_contact_id,
           c.hubspot_company_id,
           case
               when c.became_customer_date is not null and c.hubspot_company_id is not null
                   then rank() over (partition by c.hubspot_company_id order by became_customer_date asc)
               when c.hubspot_company_id is null
                   then 1
               else null end as became_company_customer_number,
           case
               when c.became_mql_date is not null and c.hubspot_company_id is not null
                   then rank() over (partition by c.hubspot_company_id order by became_mql_date asc)
               when c.hubspot_company_id is null
                   then 1
               else null end as became_company_mql_number,
           case
               when c.became_opportunity_date is not null and c.hubspot_company_id is not null
                   then rank() over (partition by c.hubspot_company_id order by became_opportunity_date asc)
               when c.hubspot_company_id is null
                   then 1
               else null end as became_company_opportunity_number
    from {{ ref('stg_contacts_lifecycle') }} c