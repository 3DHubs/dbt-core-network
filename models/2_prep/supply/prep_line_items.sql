
{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    unique_key="uuid",  
    tags=["multirefresh"],
    post_hook=["delete from {{ this }}  where uuid not in (select uuid from {{ ref('line_items') }} )"]
  )
}}


-- This model queries from int_service_supply.line_items and considerably filters the data to improve the performance of models downstream.
-- Furthermore this model is combined with a few selected fields from supply_documents (cnc_order_quotes) to facilitate identifying the 
-- characteristics of the document (quote or purchase orders) the line item belongs to. 

select  

    -- Fields from Documents
    -- Useful to filter line items from different documents and their statuses
       docs.order_uuid,
       docs.uuid             as document_uuid,
       docs.type             as document_type,
       docs.revision         as document_revision,
       docs.is_order_quote,
       docs.is_active_po,
       docs.updated          as doc_updated_at,
       docs.order_updated_at as order_updated_at,
       docs.shipping_address_id,
    -- Line Item Fields
       li.*,
    -- Part Dimensional Fields
       case when li.upload_properties is not null then 
       round(nullif(json_extract_path_text(li.upload_properties, 'volume', 'value', true), '')::float / 1000, 6)                                                 
                                                                end as upload_part_volume_cm3, -- prefix to make origin explicit
       case when li.upload_properties is not null then                                                         
       round(nullif(json_extract_path_text(li.upload_properties, 'natural_bounding_box', 'value', 'depth', true),'')::float / 10, 6)   
                                                                end as part_depth_cm,
       case when li.upload_properties is not null then
       round(nullif(json_extract_path_text(li.upload_properties, 'natural_bounding_box', 'value', 'width', true),'')::float / 10, 6)
                                                                end as part_width_cm,
       case when li.upload_properties is not null then
       round(nullif(json_extract_path_text(li.upload_properties, 'natural_bounding_box', 'value', 'height', true),'')::float / 10, 6)
                                                                end as part_height_cm,
       case when li.upload_properties is not null then
       round(nullif(json_extract_path_text(li.upload_properties, 'smallest_bounding_box', 'value', 'depth', true),'')::float / 10, 6)
                                                                end as smallest_bounding_box_depth_cm,
       case when li.upload_properties is not null then
       round(nullif(json_extract_path_text(li.upload_properties, 'smallest_bounding_box', 'value', 'width', true),'')::float / 10, 6)
                                                                end as smallest_bounding_box_width_cm,
       case when li.upload_properties is not null then
       round(nullif(json_extract_path_text(li.upload_properties, 'smallest_bounding_box', 'value', 'height', true),'')::float / 10, 6)
                                                                end as smallest_bounding_box_height_cm,                        
       round(part_depth_cm * part_width_cm * part_height_cm, 6) as part_bounding_box_volume_cm3,
       round(part_depth_cm * part_width_cm * part_height_cm, 6) as part_smallest_bounding_box_volume_cm3

from {{ ref('line_items') }} as li
 
inner join {{ ref('prep_supply_documents') }} as docs on li.quote_uuid = docs.uuid

where true
    -- Filter: only interested until now on the main quote and purchase orders
    and (is_order_quote or docs.type = 'purchase_order')    
    -- Filter: only interested on quotes that are not in the cart status
  --  and docs.status <> 'cart'

  {% if is_incremental() %}

    and (
      li.li_updated_at >= (select max(li_updated_at) from {{ this }})
      or docs.updated >= (select max(doc_updated_at) from {{ this }})
    )

  {% endif %}