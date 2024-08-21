select 
    created_at,
    user_id,
    uuid,
    country_code,
    first_name,
    last_name,
    full_name,
    email,
    settings,
    is_email_verified,
    signup_source,
    hubspot_contact_id,
    last_sign_in_at,
    is_internal,
    is_test
from {{ ref('network_services', 'gold_users') }}