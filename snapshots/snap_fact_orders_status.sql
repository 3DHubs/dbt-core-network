{% snapshot snap_fact_orders_status %}

    {{
        config(
          strategy='check',
          unique_key='order_uuid',
          check_cols=['order_status'],
        )
    }}

    select order_uuid, order_status from {{ ref('fact_orders') }}

{% endsnapshot %}