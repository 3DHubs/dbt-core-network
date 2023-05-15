select
    user_id,
    created_at,
    email,
    first_name,
    last_name,
    country_code,
    last_sign_in_at,
    hubspot_contact_id,
    rnk_desc_hubspot_contact_id,
    user_role_mapped,
    is_internal,
    is_test,
    is_active,
    company_name,
    postal_code
from {{ ref('prep_users') }}
