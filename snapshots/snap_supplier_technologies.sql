{% snapshot snap_supplier_technologies %}

{{
    config(
      strategy='check',
      unique_key='supplier_id',
      check_cols=['technology_id', 'allow_orders_with_finishes', 'allow_strategic_orders', 'strategic_orders_priority', 'min_order_amount', 'max_order_amount'],
      invalidate_hard_deletes=True,
    )
}}

select * from {{ source('int_service_supply', 'supplier_technologies') }}

{% endsnapshot %}