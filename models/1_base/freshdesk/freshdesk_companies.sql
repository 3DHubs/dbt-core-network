
select id,
       name,
       load_timestamp
from {{ ref('ingestion', 'gold_ext_airbyte_freshdesk_companies') }}