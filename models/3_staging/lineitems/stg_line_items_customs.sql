select li.uuid, 
        nvl(cc_rates.rate,0) as estimated_l1_customs_rate
        
        from {{ ref('prep_line_items')}} as li
        inner join {{ ref('prep_purchase_orders')}} po on (li.quote_uuid = po.uuid)
        inner join {{ ref('suppliers')}} s on (po.supplier_id = s.id)
        inner join  {{ source('int_service_supply', 'commodity_code_customs_rates') }}  cc_rates on (li.commodity_code = cc_rates.commodity_code and cc_rates.shipping_country_id = po.shipping_country_id and cc_rates.supplier_country_id=s.country_id)
        where true
        and type = 'part'