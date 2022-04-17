with companies as (select *,
                          row_number() over (partition by id order by load_timestamp desc nulls last) as rn
                   from {{ source('landing', 'freshdesk_companies_landing') }} )
select companies.id,
       companies.name,
       companies.description,
       companies.note,
       companies.domains,
       companies.created_at,
       companies.updated_at,
       companies.custom_fields,
       companies.health_score,
       companies.account_tier,
       companies.renewal_date,
       companies.industry,
       decode(companies.rn, 1, True) as _is_latest,
       companies.load_timestamp      as _load_timestamp
from companies