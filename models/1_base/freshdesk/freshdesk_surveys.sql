with surveys as (
    select *,
           row_number() over (partition by id, value_id order by load_timestamp nulls last) as rn
    from {{ source('ext_freshdesk', 'freshdesk_surveys') }}
)
select id,
       title,
       active,
       created_at,
       updated_at,
       value_id,
       value_label,
       value_accepted_ratings,
       value_default,
       decode(rn, 1, True) as _is_latest,
       load_timestamp      as _load_timestamp
from surveys