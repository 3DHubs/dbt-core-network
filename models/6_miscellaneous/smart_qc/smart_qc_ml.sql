{#
{{ config(materialized="incremental"
, tags=["multirefresh"]
      ,pre_hook=[""" {% if  env_var('DBT_ENVIRONMENT') == 'prod' %} 
            INSERT INTO {{ source('int_analytics', 'smart_qc_unique_history') }}
            SELECT 
            qc.line_item_uuid, 
            qc.model_executed_at, 
            qc.predicted_proba
            FROM {{ source('int_analytics', 'smart_qc') }} as qc
            inner join {{ ref('prep_line_items') }} li on li.uuid = qc.line_item_uuid
            inner join {{ ref('stg_orders_logistics') }}  fo on fo.order_uuid = li.order_uuid
            where fo.shipped_at is not null
            and line_item_uuid NOT IN (
                SELECT
                    line_item_uuid
            FROM {{ source('int_analytics', 'smart_qc_unique_history') }})
               {% endif %} """]

    ,post_hook = """ {% if  env_var('DBT_ENVIRONMENT') == 'prod' %} 
            unload ('SELECT line_item_uuid, predicted_proba FROM  {{ this }} where loaded_at = \\'{{ run_started_at }}\\' ') 
            TO 's3://controlhub.prod.hubs.com/scan-hub/smart-qc-ml/{{ run_started_at.strftime(\"%Y-%m-%d-%H:%M\") }}/'
            IAM_ROLE 'arn:aws:iam::256629611817:role/redshift-spectrum'
            parquet
            maxfilesize 100 mb
            {% endif %}
    """
    )
 }}
#}

-- -- This model makes sure that the upsert int_analytics.smart_qc only stores the result the moment the shipped at is known.
-- select line_item_uuid, 
--        predicted_proba,
--        model_executed_at,
--        '{{ run_started_at }}' as loaded_at
--     from {{ source("int_analytics", "smart_qc_unique_history") }} qc   
--     where true
   
--       {% if is_incremental() %}

--              and line_item_uuid not in (select distinct line_item_uuid from {{ this }})

--      {% endif %}
