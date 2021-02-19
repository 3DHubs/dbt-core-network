{% snapshot snap_bids %}

    {{
        config(
          strategy='timestamp',
          unique_key='uuid',
          updated_at='updated',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ source('int_service_supply', 'bids') }}

{% endsnapshot %}