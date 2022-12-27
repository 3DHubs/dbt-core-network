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
            rank() over (partition by hubspot_contact_id order by created_at asc) as rnk_asc_hubspot_contact_id    
             
            from {{ ref('fact_orders') }}
    ),
    empty_cart as (
        select distinct
        users.hubspot_contact_id
        from {{ source('int_service_supply', 'cnc_orders') }} orders
        left join {{ ref('users') }} as users on users.user_id = orders.user_id
        where orders.uuid not in (
        select uuid from {{ ref('prep_supply_orders') }})
    )
    select hc.contact_id,
           least(
               legacy.first_upload_at,
               hc.hs_lifecyclestage_marketingqualifiedlead_date,
               became_cart_date) as mql_date,
               technology_name as mql_technology,
               case when mql_date >= '2022-01-01' then 
               case when opportunity.hubspot_contact_id is not null  then 'cart' 
                    when empty_cart.hubspot_contact_id is not null then 'empty_cart' else 'subscriber' end end as mql_type
    from {{ ref('stg_hs_contacts_union_legacy') }} hc
            left join legacy on legacy.email = hc.email
            left join opportunity on opportunity.hubspot_contact_id = hc.contact_id and rnk_asc_hubspot_contact_id = 1
            left join empty_cart on empty_cart.hubspot_contact_id = hc.contact_id
    group by 1,2,3,4