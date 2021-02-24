select oqs.created,
       oqs.updated,
       oqs.deleted,
       po.uuid,
       po.supplier_document_number,
       po.status,
       po.supplier_support_ticket_id,
       po.packing_slip_uuid,
       decode(po.is_created_manually, 'false', False, 'true', True) as is_created_manually,
       po.voided_at,
       po.supplier_id,
       po.author_id,
       po.billing_request_id
from int_service_supply.purchase_orders as po
         inner join {{ ref('cnc_order_quotes') }} as oqs
                    on po.uuid = oqs.uuid