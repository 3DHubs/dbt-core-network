{% snapshot snap_bids %}

    {{
        config(
          strategy='timestamp',
          unique_key='uuid',
          updated_at='updated',
          invalidate_hard_deletes=True,
        )
    }}

    select bids.*
    from {{ source('int_service_supply', 'new_bids') }} as bids

{% endsnapshot %}