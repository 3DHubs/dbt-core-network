{{
    config(
        materialized='table'
    )
}}

with user_role_mapping as (
    select user_id,
        min(case when name = 'supplier' then 'supplier' else 'hubs' end) as role_mapped -- a user can have multiple roles for Hubs employees. Min gives prio to Hubs
    from {{ ref('user_roles') }}
    group by 1
),
-- prep company name of address to be added to users for Protolabs
user_comp as (
select
    email, 
    company_name, 
    postal_code,
    row_number() over (partition by email order by created desc, updated desc) as rnk_desc_comp
        from  {{ ref('addresses') }} 
)

select distinct
    created_at,
    users.user_id,
    uuid,
    country_code,
    first_name,
    last_name,
    full_name,
    users.email,
    settings,
    is_email_verified,
    signup_source,
    hubspot_contact_id,
    last_sign_in_at,
    datediff('day', last_sign_in_at, current_date) as last_sign_in_at_days_ago,
    case
        when last_sign_in_at_days_ago >= 365 or not last_sign_in_at_days_ago then False
        else True
    --    else decode(is_active, 'true', True, 'false', False) -- This seems to have been dropped without communication.
        end                                                               is_active,
    is_internal,
    is_test,
    user_comp.company_name,
    user_comp.postal_code,
    coalesce(dur.role_mapped, 'customer') as user_role_mapped,
    hcon.associatedcompanyid as hubspot_company_id,
    rank() over (partition by hubspot_contact_id order by created_at desc) as rnk_desc_hubspot_contact_id
from {{ ref('users') }} users
left join {{ ref('hubspot_contacts') }} as hcon on hubspot_contact_id = hcon.contact_id
left join user_role_mapping as dur on users.user_id = dur.user_id
left join user_comp on users.email = user_comp.email and rnk_desc_comp = 1