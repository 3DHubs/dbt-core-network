select
uuid,
created,
updated,
supplier_id,
user_id,
email,
email_domain,
is_hubs,
is_internal,
is_protolabs,
is_anonymized,
last_active_at

from {{ ref('sources_network', 'gold_supplier_users') }}