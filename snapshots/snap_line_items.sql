{% snapshot snap_line_items %}

    {{
        config(
          strategy='timestamp',
          unique_key='id',
          updated_at='updated',
          invalidate_hard_deletes=True,
        )
    }}

    select * from {{ ref('hubs', 'line_items') }}

{% endsnapshot %}