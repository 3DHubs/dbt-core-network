{% snapshot snap_countries %}

{{
    config(
      strategy='timestamp',
      unique_key='country_id',
      updated_at='updated',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('int_service_supply', 'countries') }}

{% endsnapshot %}