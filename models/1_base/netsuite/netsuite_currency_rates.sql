{{ config(bind=False) }}

select *
from {{ source('ext_netsuite', 'currencyrate') }}