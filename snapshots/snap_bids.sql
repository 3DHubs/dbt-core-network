{% snapshot snap_bids %}

    {{
        config(
          strategy='timestamp',
          unique_key='uuid',
          updated_at='updated',
          invalidate_hard_deletes=True,
        )
    }}

    select oqs.created,
           oqs.updated,
           oqs.deleted,
           bids.*
    from {{ source('int_service_supply', 'bids') }} as bids
    inner join {{ source('int_service_supply', 'cnc_order_quotes') }} as oqs
               on bids.uuid = oqs.uuid

{% endsnapshot %}