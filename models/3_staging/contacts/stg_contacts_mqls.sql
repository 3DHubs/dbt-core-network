with legacy as (
            select email, 
            first_upload_at
            from {{ source('data_lake', 'legacy_mqls') }}
    ),
    opportunity as (
            select 
            hubspot_contact_id, 
            created_at as became_cart_date,
            technology_name,
            integration_platform_type,
            rank() over (partition by hubspot_contact_id order by created_at asc) as rnk_asc_hubspot_contact_id    
             
            from {{ ref('fact_orders') }}
    )
    select hc.contact_id,
           least(
               legacy.first_upload_at,
               hc.hs_lifecyclestage_marketingqualifiedlead_date,
               became_cart_date) as mql_date,
               technology_name as mql_technology,
               case when mql_date >= '2022-01-01' then 
               case when integration_platform_type is not null then integration_platform_type else 'cart' end else 'legacy' end as mql_type
    from {{ ref('stg_hs_contacts_union_legacy') }} hc
            left join legacy on legacy.email = hc.email
            left join opportunity on opportunity.hubspot_contact_id = hc.contact_id and rnk_asc_hubspot_contact_id = 1
            where least(
               legacy.first_upload_at,
               hc.hs_lifecyclestage_marketingqualifiedlead_date,
               became_cart_date) < '2022-01-01' or (least(
               legacy.first_upload_at,
               hc.hs_lifecyclestage_marketingqualifiedlead_date,
               became_cart_date) >= '2022-01-01' and  opportunity.hubspot_contact_id is not null)
    group by 1,2,3,4