{{ config(materialized="incremental") }}
-- This model makes sure that the upsert int_analytics.smart_qc only stores the result the moment the shipped at is known.
select qc.line_item_uuid, 
       qc.predicted_proba,
       qc.model_executed_at
    from {{ source("int_analytics", "smart_qc") }} qc
    inner join {{ ref("prep_line_items") }} li on li.uuid = qc.line_item_uuid
    inner join {{ ref("stg_orders_logistics") }}  fo on fo.order_uuid = li.order_uuid
    where fo.shipped_at is not null
   
      {% if is_incremental() %}

             and predicted_proba not in (select distinct predicted_proba from {{ this }})

     {% endif %}
