{{
    config(
        materialized='table'
    )
}}
with
    delivered as (
        select distinct recipient
        from {{ source('ext_hubspot', 'email_events') }}
        where type = 'DELIVERED' and sentby__created >= '2023-01-01'
    )
select distinct recipient
from {{ source('ext_hubspot', 'email_events') }}
where
    type in ('DEFERRED', 'BOUNCE')
    and sentby__created >= '2023-01-01'
    and recipient not in (select recipient from delivered)
