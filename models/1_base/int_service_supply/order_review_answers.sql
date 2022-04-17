with answers as (
    select *, row_number() over (partition by order_uuid, question_id order by created desc nulls last) as rn
    from {{ source('int_service_supply', 'order_review_answers') }}
)
select *
from answers
where rn = 1