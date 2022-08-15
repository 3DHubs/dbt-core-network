select 
       oqs.order_uuid,
       oqs.created,
       oqs.updated,
       oqs.deleted,
       oqs.shipping_address_id,
       po.uuid,
       po.supplier_document_number,
       po.status,
       po.supplier_support_ticket_id,
       po.packing_slip_uuid,
       {{ varchar_to_boolean('is_created_manually') }}, -- From `po`
       po.voided_at,
       po.supplier_id,
       po.author_id,
       po.billing_request_id

from {{ source('int_service_supply', 'purchase_orders') }} as po
         inner join {{ ref('prep_supply_documents') }} as oqs
                    on po.uuid = oqs.uuid