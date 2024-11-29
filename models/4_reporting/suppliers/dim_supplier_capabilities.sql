with supplier_tech as (
    select s.id as supplier_id,
           s.name as supplier_name,
           st.technology_id,
           st.technology_name,
           st.min_order_amount_usd,
           st.max_order_amount_usd,
           st.max_active_orders,
           st.allow_cosmetic_worthy_finishes,
           st.allow_orders_with_custom_finishes,
           st.num_parts_min, 
           st.num_parts_max, 
           st.num_units_min, 
           st.num_units_max,
           cl.business_classification,
           cl.service_level_classification
    from {{ ref('suppliers') }} as s -- Note: using suppliers as main means including some suppliers that don't exist in supplier_technologies
             left join {{ ref('supplier_technologies') }} as st on s.id = st.supplier_id
             left outer join {{ ref('supplier_users') }} as ssu on st.supplier_id = ssu.supplier_id
             left join {{ ref('seed_supplier_business_classification')}} cl on cl.supplier_id =  st.supplier_id and cl.technology_name = st.technology_name
    where not ssu.is_hubs
),
     finishes_prep as (
         select sf.supplier_id,
                sf.supplier_name,
                sf.surface_finish_id,
                sf.surface_finish_name,
                mf.technology_id,
                mf.material_name,
                mf.material_id
         from {{ ref('supplier_finishes') }} sf
                  -- Finishes and materials have a m-to-m relationship
                  left join {{ ref('material_finishes') }} mf on mf.id = sf.surface_finish_id
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
         select sms.supplier_id,
                sms.supplier_name,
                sms.material_subset_name,
                sms.material_subset_id,
                sms.is_available_in_auctions,
                sms.material_excluded_in_eu,
                sms.material_excluded_in_us,
                sms.technology_id,
                sms.material_id,
                sms.process_id
         from {{ ref('supplier_material_subsets') }} as sms
     ),
     processes as (
         select sp.supplier_id,
                sp.supplier_name,
                sp.technology_id,
                sp.process_id,
                sp.process_name,
                sp.depth_min, 
                sp.depth_max, 
                sp.width_min, 
                sp.width_max, 
                sp.height_min, 
                sp.height_max
         from {{ ref('supplier_processes') }} as sp
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
         left join material_subsets ms on ms.supplier_id = st.supplier_id and ms.technology_id = st.technology_id
         left join processes p on p.supplier_id = st.supplier_id and st.technology_id = p.technology_id and
                                  st.technology_id <> 2 -- processes independent for non-3DP orders
         left join processes p1 on p1.supplier_id = st.supplier_id and ms.process_id = p1.process_id and
                                   st.technology_id = 2 -- processes not independent for 3DP