{% snapshot snap_suppliers %}

{{
    config(
      strategy='timestamp',
      unique_key='id',
      updated_at='updated',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('int_service_supply', 'suppliers') }}

{% endsnapshot %}