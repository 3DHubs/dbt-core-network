{% snapshot snap_dim_companies_hubspot_owner %}

    {{
        config(
          strategy='check',
          unique_key='hubspot_company_id',
          check_cols=['hubspot_owner_id'],
        )
    }}

    select hubspot_company_id, hubspot_owner_id from {{ ref('dim_companies') }}

{% endsnapshot %}