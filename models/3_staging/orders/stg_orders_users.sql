-- For all orders that don't exist in Hubspot (carts under a certain value) we prep the association of hubspot contacts to still link the cart to hubspot.

select 
    orders.uuid                                                     as order_uuid,
    coalesce(hcon.contact_id, users.hubspot_contact_id)             as hubspot_contact_id,
    coalesce(hcon.associatedcompanyid, users.hubspot_company_id)    as hubspot_company_id
from {{ ref('prep_supply_orders') }} as orders
left join {{ ref('hubspot_contacts') }} as hcon on hcon.first_cart_uuid = orders.uuid and rnk_asc_cart = 1
left join {{ ref('users') }} as users on users.user_id = orders.user_id and is_internal = false -- to exclude admin created quotes