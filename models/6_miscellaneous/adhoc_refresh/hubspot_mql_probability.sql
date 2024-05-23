-- This model runs multiple times a day to spread the new MQLs with customer prediction for Zapier / Hubspot integration. Else it cannot handle the load.
-- Define by JG May 2024
{{
    config(
        materialized='incremental',
        unique_key='hubspot_contact_id',
    )
}}
select 
    hubspot_contact_id,
    is_customer_prediction::decimal(4,2) as customer_prediction 
from {{ ref('stg_mql_clv') }}
where became_mql_at >= '2024-05-01' and is_customer_prediction > 0.01
    
    {% if is_incremental() %}

  -- this filter will only be applied on an incremental run
    and hubspot_contact_id not in (select hubspot_contact_id from {{ this }} )

    {% endif %}

limit 100
