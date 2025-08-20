with legacy as (
            select email, 
            first_upload_at
            from {{ source('int_analytics', 'legacy_mqls') }}
    ),
    cart as (
            select 
            hubspot_contact_id, 
            created_at as became_cart_date,
            technology_name,
            integration_platform_type,
            rank() over (partition by hubspot_contact_id order by created_at asc) as rnk_asc_hubspot_contact_id    
             
            from {{ ref('fact_orders') }}
    ),  
    deleted_cart as (
        select distinct
        contact_id
        from {{ ref('stg_hs_contacts_attributed_prep') }} contacts
        where first_cart_uuid not in (
        select uuid from {{ ref('prep_supply_orders') }})
    ),    
    empty_cart_1 as (
        select distinct
        contact_id
        from {{ ref('stg_hs_contacts_attributed_prep') }} contacts
        where first_cart_uuid not in (
        select uuid from {{ ref('orders') }})
    ),    
    empty_cart_2 as (
        select distinct
        users.hubspot_contact_id
        from {{ ref('orders') }} orders
        left join {{ ref('prep_users') }} as users on users.user_id = orders.user_id
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
               case when regexp_like(lower(hc.hutk_analytics_first_url), 'shallow') then 'shallowlink' --todo-migration-test replaced ~ 
                    when regexp_like(lower(hc.hutk_analytics_first_url), 'quicklink') then 'quicklink' --todo-migration-test replaced ~
                    when integration_platform_type is not null then integration_platform_type 
                    when cart.hubspot_contact_id is not null  then 'cart'  
                    when deleted_cart.contact_id is not null then 'deleted_cart'
                    when empty_cart_1.contact_id is not null then 'empty_cart'
                    when empty_cart_2.hubspot_contact_id is not null then 'empty_cart' else 'unknown_cart_details' end else 'legacy' end as mql_type
    from {{ ref('stg_hs_contacts_attributed_prep') }} hc
            left join legacy on legacy.email = hc.email
            left join cart on cart.hubspot_contact_id = hc.contact_id and rnk_asc_hubspot_contact_id = 1
            left join deleted_cart on deleted_cart.contact_id = hc.contact_id
            left join empty_cart_1 on empty_cart_1.contact_id = hc.contact_id
            left join empty_cart_2 on empty_cart_2.hubspot_contact_id = hc.contact_id
            -- This code could be reconsidered end of year to remove empty carts and subscribers as mql type from the total. This was reverted in May after talking to Merrit. 
            -- where least( 
            --    legacy.first_upload_at,
            --    hc.hs_lifecyclestage_marketingqualifiedlead_date,
            --    became_cart_date) < '2022-01-01' or (least(
            --    legacy.first_upload_at,
            --    hc.hs_lifecyclestage_marketingqualifiedlead_date,
            --    became_cart_date) >= '2022-01-01' and  opportunity.hubspot_contact_id is not null)
    group by 1,2,3,4