{% snapshot snap_users %}

    {{
        config(
          strategy='check',
          unique_key='user_id',
          check_cols=['last_sign_in_at'],
        )
    }}

    select user_id, last_sign_in_at::timestamp as last_sign_in_at from {{ ref('prep_users') }} where last_sign_in_at::timestamp is not null

{% endsnapshot %}