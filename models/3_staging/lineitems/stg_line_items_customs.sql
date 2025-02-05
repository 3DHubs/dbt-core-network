select li.uuid, 
        nvl(snapcc.rate,0) as estimated_l1_customs_rate
        
        from {{ ref('prep_line_items')}} as li
        inner join {{ ref('prep_purchase_orders')}} po on (li.quote_uuid = po.uuid)
        inner join {{ ref('suppliers')}} s on (po.supplier_id = s.id)
        inner join  {{ ref('snap_commodity_code') }}  snapcc on (li.commodity_code = snapcc.commodity_code and snapcc.shipping_country_id = po.shipping_country_id
                    and snapcc.supplier_country_id=s.country_id and  po.created >= snapcc.dbt_valid_from and po.created < coalesce(snapcc.dbt_valid_to, getdate()))
        where true
        and type = 'part'

       