{% snapshot snap_commodity_code %}

{{
    config(
      strategy='timestamp',
      unique_key='id',
      updated_at='updated_at',
    )
}}

select * from {{ ref('sources_network', 'gold_commodity_code_customs_rates') }}

{% endsnapshot %}