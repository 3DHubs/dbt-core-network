{{
    config(
        materialized='table'
    )
}}

with 
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
    u.created_at,
    u.user_id,
    u.uuid,
    u.country_code,
    u.first_name,
    u.last_name,
    u.full_name,
    u.email,
    case when u.user_type = 'employee' then 'hubs' else u.user_type end as user_role_mapped,
    u.email_domain,
    u.is_internal,
    u.is_test,
    u.is_protolabs,
    u.is_anonymized,
    u.settings,
    u.is_email_verified,
    u.signup_source,
    u.hubspot_contact_id,
    u.last_sign_in_at,
    u.team_id,
    u.team_name,
    u.team_created_at,
    datediff('day', u.last_sign_in_at, current_date) as last_sign_in_at_days_ago,
    case
        when last_sign_in_at_days_ago >= 365 or not last_sign_in_at_days_ago then False
        else True
    --    else decode(is_active, 'true', True, 'false', False) -- This seems to have been dropped without communication.
        end                                                               is_active,
    user_comp.company_name,
    user_comp.postal_code,
    hcon.associatedcompanyid as hubspot_company_id,
    rank() over (partition by hubspot_contact_id order by created_at desc) as rnk_desc_hubspot_contact_id
from {{ ref('users') }} as u
left join {{ ref('hubspot_contacts') }} as hcon on hubspot_contact_id = hcon.contact_id
left join user_comp on u.email = user_comp.email and rnk_desc_comp = 1