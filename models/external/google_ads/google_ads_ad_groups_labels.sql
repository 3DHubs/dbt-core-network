{{ config(bind=False) }}

select *
from {{ source('ext_adwords', 'ad_groups__labels') }}