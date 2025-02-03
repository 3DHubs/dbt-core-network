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
       po.billing_request_id,
       oqs.shipping_locality,
       oqs.shipping_latitude,
       oqs.shipping_longitude,
       oqs.shipping_country_id,
       oqs.shipping_country

from {{ ref('network_services', 'gold_purchase_orders') }} as po
         inner join {{ ref('prep_supply_documents') }} as oqs
                    on po.uuid = oqs.uuid