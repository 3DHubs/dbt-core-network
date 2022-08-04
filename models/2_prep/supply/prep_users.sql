-- Enriches users data with roles mapped as customer, hubs and supplier

with user_role_mapping as (
    select ur.user_id,
        case when r.name = 'supplier' then 'supplier' else 'hubs' end as role_mapped
    from {{ source('int_service_supply', 'users_roles') }} as ur
    left join {{ source('int_service_supply', 'roles') }} as r on ur.role_id = r.id
-- A user can have multiple roles assigned (for Hubs employees)
), distinct_user_role as (
    select distinct user_id, 
        role_mapped 
    from user_role_mapping
)
select u.*,
-- Customers have no roles assigned
       coalesce(dur.role_mapped, 'customer') as user_role_mapped
from {{ source('int_service_supply', 'users') }} as u
left join distinct_user_role as dur on u.user_id = dur.user_id