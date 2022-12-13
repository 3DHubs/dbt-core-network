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
    -- Line Item Fields
       li.*

from {{ ref('line_items') }} as li
 
inner join {{ ref('prep_supply_documents') }} as docs on li.quote_uuid = docs.uuid
where true
    -- Filter: only interested until now on the main quote and purchase orders
    and (is_order_quote or docs.type = 'purchase_order')    
    -- Filter: only interested on quotes that are not in the cart status
  --  and docs.status <> 'cart'

