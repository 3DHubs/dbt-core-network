{% snapshot snap_cnc_order_quotes %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updated',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('hubs', 'cnc_order_quotes') }}

{% endsnapshot %}