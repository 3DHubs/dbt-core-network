{% snapshot snap_rfm %}

    {{
        config(
          strategy='check',
          unique_key='hubspot_company_id',
          check_cols=['r_score','f_score','m_score','rfm_segment'],
        )
    }}

select *
from {{ ref('agg_orders_companies_rfm') }}

{% endsnapshot %}