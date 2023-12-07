 select li.uuid, 
        nvl(cc_rates.rate,0) as estimated_l1_customs_rate
        
        from {{ ref('prep_line_items')}} as li
        inner join {{ ref('prep_purchase_orders')}} po on (li.quote_uuid = po.uuid)
        inner join {{ ref('addresses')}} destination_address on (destination_address.address_id = li.shipping_address_id)
        inner join {{ ref('prep_countries')}} destination_country on (destination_address.country_id = destination_country.country_id)
        inner join {{ ref('suppliers')}} s on (po.supplier_id = s.id)
        inner join {{ ref('addresses')}} supplier_address on (supplier_address.address_id = s.address_id)
        inner join {{ ref('prep_countries')}} supplier_country on (supplier_address.country_id = supplier_country.country_id)
        inner join  {{ source('int_service_supply', 'commodity_code_customs_rates') }}  cc_rates on (li.commodity_code = cc_rates.commodity_code and cc_rates.shipping_country_id = destination_address.country_id and cc_rates.supplier_country_id=supplier_address.country_id)
        where true
        and type = 'part'