select sup.id                                                 as supplier_id,
       tec.technology_id,
       tec.name                                               as technology_name,
       matsub.slug                                            as material,
       ((supt.min_order_amount::float) / 100)::decimal(15, 2) as min_order_amount_usd,
       ((supt.max_order_amount::float) / 100)::decimal(15, 2) as max_order_amount_usd,
       supt.max_active_orders,
       supt.allow_orders_with_custom_finishes
from {{ ref('suppliers') }} as sup
            left outer join {{ ref('supplier_technologies') }} as supt on sup.id = supt.supplier_id
            left outer join {{ ref('technologies') }} as tec on supt.technology_id = tec.technology_id
            left outer join {{ source('int_service_supply', 'suppliers_material_subsets') }} as supmatsub
                            on supmatsub.supplier_id = sup.id
            left outer join {{ ref('material_subsets') }} as matsub
                            on matsub.material_subset_id = supmatsub.material_subset_id