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
            case when number_of_part_line_items is null then 'empty_cart' else 'cart' end as mql_type,
            rank() over (partition by hubspot_contact_id order by created_at asc) as rnk_asc_hubspot_contact_id    
             
            from {{ ref('fact_orders') }}
                         )
    select hc.contact_id,
           least(
               legacy.first_upload_at,
               hc.hs_lifecyclestage_marketingqualifiedlead_date,
               became_cart_date) as mql_date,
               technology_name as mql_technology,
               case when mql_date >= '2022-01-01' then case when opportunity.hubspot_contact_id is null  then 'subscriber' else mql_type end end as mql_type
    from {{ ref('stg_hs_contacts_union_legacy') }} hc
            left join legacy on legacy.email = hc.email
            left join opportunity on opportunity.hubspot_contact_id = hc.contact_id and rnk_asc_hubspot_contact_id = 1
    group by 1,2,3,4