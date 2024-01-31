with aue as 
    (
    select
        order_uuid, 
        anonymous_user_email,
        row_number() over (partition by order_uuid order by created desc) as rn
    from {{ source('int_service_supply', 'anonymous_user_carts') }}
    ) 
select
    order_uuid, 
    anonymous_user_email
from aue 
where aue.rn = 1
