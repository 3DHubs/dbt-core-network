
select id,
       name,
       load_timestamp
from {{ ref('dbt_src_external', 'gold_airbyte_freshdesk_companies') }}