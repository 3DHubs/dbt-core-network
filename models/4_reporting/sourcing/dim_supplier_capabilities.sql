with supplier_tech as (
    select s.id                                                 as supplier_id,
           s.name                                               as supplier_name,
           st.technology_id,
           tec.name                                             as technology_name,
           ((st.min_order_amount::float) / 100)::decimal(15, 2) as min_order_amount_usd,
           ((st.max_order_amount::float) / 100)::decimal(15, 2) as max_order_amount_usd,
           st.max_active_orders,
           st.allow_strategic_orders,
           st.allow_non_strategic_orders,
           st.num_parts_min, 
           st.num_parts_max, 
           st.num_units_min, 
           st.num_units_max,
           cl.business_classification,
           cl.service_level_classification
    from {{ ref('suppliers') }} s
             left join {{ ref('supplier_technologies') }} st on st.supplier_id = s.id
             left outer join {{ ref('technologies') }} as tec on st.technology_id = tec.technology_id
             left outer join {{ source('int_service_supply', 'supplier_users') }} as ssu on s.id = ssu.supplier_id
             left outer join {{ ref('users') }} as su on ssu.user_id = su.user_id
             left join {{ ref('seed_supplier_business_classification')}} cl on cl.supplier_id =  s.id and cl.technology_name = tec.name
    where su.mail !~ '@(3d)?hubs.com'
),
     finishes_prep as (
         select s.id          as supplier_id,
                s.name        as supplier_name,
                sf.finish_id  as surface_finish_id,
                mf.name       as surface_finish_name,
                m.technology_id,
                m.name        as material_name,
                m.material_id as material_id
         from {{ ref('suppliers') }} s
                  left join {{ source('int_service_supply', 'supplier_finishes') }} sf on sf.supplier_id = s.id
                  left join {{ ref('material_finishes') }} mf on mf.id = sf.finish_id
                  left join {{ source('int_service_supply', 'materials_material_finishes') }} mmf
                            on mmf.material_finish_id = sf.finish_id
                  left join {{ ref('materials') }} m on mmf.material_id = m.material_id
     ),
     finishes as (
         select supplier_id,
                supplier_name,
                technology_id,
                surface_finish_name,
                surface_finish_id
         from finishes_prep
         group by 1, 2, 3, 4, 5
     ),
     material_subsets as (
         select s.id,
                s.name  as supplier_name,
                ms.name as material_subset_name,
                ms.material_subset_id,
                ms.is_available_in_auctions,
                ms.material_excluded_in_eu,
                ms.material_excluded_in_us,
                m.technology_id,
                m.material_id,
                ms.process_id
         from {{ ref('suppliers') }} as s
                  left outer join {{ source('int_service_supply', 'suppliers_material_subsets') }} as sms on s.id = sms.supplier_id
                  left outer join {{ ref('prep_material_subsets') }} as ms
                                  on sms.material_subset_id = ms.material_subset_id
                  left join {{ ref('materials') }} m on ms.material_id = m.material_id
     ),
     processes as (
         select id     as supplier_id,
                s.name as supplier_name,
                p.technology_id,
                p.process_id,
                p.name as process_name,
                sp.depth_min, 
                sp.depth_max, 
                sp.width_min, 
                sp.width_max, 
                sp.height_min, 
                sp.height_max
         from {{ ref('suppliers') }} as s
                  left outer join {{ source('int_service_supply', 'supplier_processes') }} as sp on s.id = sp.supplier_id
                  left outer join {{ ref('processes') }} as p on sp.process_id = p.process_id
     )
select st.*,
       f.surface_finish_id,
       f.surface_finish_name,
       ms.material_subset_name,
       ms.material_subset_id,
       ms.is_available_in_auctions,
       ms.material_excluded_in_eu,
       ms.material_excluded_in_us,
       coalesce(p.process_name, p1.process_name) as process_name,
       coalesce(p.process_id, p1.process_id)     as process_id,
       p.depth_min, --cnc only
       p.depth_max,
       p.width_min,
       p.width_max,
       p.height_min,
       p.height_max
from supplier_tech st
         left join finishes f on f.supplier_id = st.supplier_id and f.technology_id = st.technology_id
         left join material_subsets ms on ms.id = st.supplier_id and ms.technology_id = st.technology_id
         left join processes p on p.supplier_id = st.supplier_id and st.technology_id = p.technology_id and
                                  st.technology_id <> 2 -- processes independent for non-3DP orders
         left join processes p1 on p1.supplier_id = st.supplier_id and ms.process_id = p1.process_id and
                                   st.technology_id = 2 -- processes not independent for 3DP