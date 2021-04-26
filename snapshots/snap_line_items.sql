{% snapshot snap_line_items %}

{{
    config(
      strategy='timestamp',
      unique_key='id',
      updated_at='updated',
      invalidate_hard_deletes=True,
      enabled=False
    )
}}

select * from {{ source('int_service_supply', 'line_items') }}

{% endsnapshot %}