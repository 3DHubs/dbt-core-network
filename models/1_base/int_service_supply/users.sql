select created,
       updated,
       deleted,
       user_id,
       uuid,
       initials,
       country_code,
       first_name,
       last_name,
       trim(trim(initcap(first_name)) || ' ' || trim(initcap(last_name))) as full_name,
       locality,
       mail                                                               as mail,
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
       rank() over (partition by hubspot_contact_id order by created desc) as rnk_desc_hubspot_contact_id    

from {{ source('int_service_supply', 'users') }} users
left join {{ ref('hubspot_contacts') }} as hcon on hubspot_contact_id = hcon.contact_id