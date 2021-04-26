{% snapshot snap_supplier_technologies %}

{{
    config(
      strategy='check',
      unique_key='supplier_id',
      check_cols=['_supplier_attr_sk'],
    )
}}

select *
from {{ ref('supplier_technologies') }}

{% endsnapshot %}