-- Ideally this model gets moved to prep_users.

with user_role_mapping as (
    select ur.user_id,
        min(case when r.name = 'supplier' then 'supplier' else 'hubs' end) as role_mapped -- a user can have multiple roles for Hubs employees. Min gives prio to Hubs
    from {{ source('int_service_supply', 'users_roles') }} as ur
    left join {{ source('int_service_supply', 'roles') }} as r on ur.role_id = r.id
    group by 1
),
-- prep company name of address to be added to users for Protolabs
user_comp as (
select
    email, 
    company_name, 
    postal_code, 
    row_number() over (partition by email order by created desc, updated desc)  as rnk_desc_comp
    from  {{ ref('addresses') }} 
)
select created as created_at,
       updated,
       deleted,
       users.user_id,
       uuid,
       initials,
       country_code,
       first_name,
       last_name,
       trim(trim(initcap(first_name)) || ' ' || trim(initcap(last_name))) as full_name,
       locality,
       mail                                                               as email,
       name                                                               as username,
       persona,
       md5(users.phone)                                                   as phone,
       picture_id,
       decode(settings, 'null', null, settings)                           as settings,
       timezone,
       url,
       email_verification_token,
       decode(is_email_verified, 'true', True, 'false', False)            as is_email_verified,
       signup_source,
       decode(hubspot_contact_id, 0, null, hubspot_contact_id)            as hubspot_contact_id,
       hcon.associatedcompanyid                                           as hubspot_company_id,
       session_invalidated_at,
       last_sign_in_at,
       datediff('day', last_sign_in_at, current_date)                     as last_sign_in_at_days_ago,
       case
           when last_sign_in_at_days_ago >= 365 or not last_sign_in_at_days_ago then False
           else True
        --    else decode(is_active, 'true', True, 'false', False) -- This seems to have been dropped without communication.
           end                                                               is_active,
       mail ~ '@(3d)?hubs.com' or mail ~ '@pthubs.com'                     as is_internal,
       mail ~ '@pthubs.com'  or mail ~ 'test@hubs.com'                     as is_test,
       user_comp.company_name,
       user_comp.postal_code,
       coalesce(dur.role_mapped, 'customer') as user_role_mapped,
       rank() over (partition by hubspot_contact_id order by created desc) as rnk_desc_hubspot_contact_id    
from {{ source('int_service_supply', 'users') }} users
left join {{ ref('hubspot_contacts') }} as hcon on hubspot_contact_id = hcon.contact_id
left join user_role_mapping as dur on users.user_id = dur.user_id
left join user_comp on users.mail = user_comp.email and rnk_desc_comp = 1