{% snapshot snap_users %}

    {{
        config(
          strategy='check',
          unique_key='user_id',
          check_cols=['last_sign_in_at'],
        )
    }}

    select user_id, last_sign_in_at from {{ ref('users') }}

{% endsnapshot %}