{{ config(materialized="incremental"
, tags=["multirefresh"]
    ,post_hook = """
            unload ('SELECT line_item_uuid, predicted_proba FROM  {{ this }} where loaded_at = \\'{{ run_started_at }}\\' ') 
            to 's3://controlhub.prod.hubs.com/scan-hub/smart-qc-ml/{{ run_started_at.strftime(\"%Y-%m-%d\") }}/'
            iam_role 'arn:aws:iam::256629611817:role/redshift-spectrum'
            parquet
            maxfilesize 100 mb
    """
    )
 }}


-- This model makes sure that the upsert int_analytics.smart_qc only stores the result the moment the shipped at is known.
select distinct qc.line_item_uuid, 
       qc.predicted_proba,
       qc.model_executed_at,
       '{{ run_started_at }}' as loaded_at
    from {{ source("int_analytics", "smart_qc") }} qc   
    inner join {{ ref("prep_line_items") }} li on li.uuid = qc.line_item_uuid
    inner join {{ ref("stg_orders_logistics") }}  fo on fo.order_uuid = li.order_uuid
    where fo.shipped_at is not null
   
      {% if is_incremental() %}

             and line_item_uuid not in (select distinct line_item_uuid from {{ this }})

     {% endif %}
