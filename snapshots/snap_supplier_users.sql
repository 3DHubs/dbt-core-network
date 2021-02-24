{% snapshot snap_supplier_users %}

{{
    config(
      strategy='timestamp',
      unique_key='user_id',
      updated_at='updated',
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('int_service_supply', 'supplier_users') }}

{% endsnapshot %}