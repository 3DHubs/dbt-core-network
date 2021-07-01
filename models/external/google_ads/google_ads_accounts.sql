{{ config(bind=False) }}

select *
from {{ source('ext_adwords', 'accounts') }}