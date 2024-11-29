-- todo: would be good to have a unique identifier

select sup.id                                                 as supplier_id,
       supt.technology_id,
       supt.technology_name,
       supt.min_order_amount_usd,
       supt.max_order_amount_usd,
       supt.max_active_orders,
       supt.allow_orders_with_custom_finishes,
       supmatsub.material_subset_slug                         as material
       
from {{ ref('suppliers') }} as sup
            left outer join {{ ref('supplier_technologies') }} as supt on sup.id = supt.supplier_id
            left outer join {{ ref('supplier_material_subsets') }} as supmatsub on supmatsub.supplier_id = sup.id