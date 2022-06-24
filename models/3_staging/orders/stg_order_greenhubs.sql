-- In this query the emissions regarding material procurement (manufacturing of the raw material) and the manufacturing of the part.
-- The calculations are based on assumptions made regarding emissions per kwh of energy consumed varying per country as well as the
-- emissions per kg of raw material per country. The result are two columns which are later combined in stg_fact_orders.



with base_query as (
-- In this part the required line item data from our DB is derived
    select li.order_uuid,
           coalesce(li.material_name, 'Other')                                  as material,
           coalesce(c.name, 'Other')                                            as country,
           line_item_weight_g,
           line_item_total_bounding_box_volume_cm3,
           material_density_g_cm3,
           line_item_total_bounding_box_volume_cm3 - line_item_total_volume_cm3 as li_total_removed_volume_cm3
    from {{ ref ('fact_line_items') }} as li
             left join {{ ref ('stg_fact_orders') }} as orders on li.order_uuid = orders.order_uuid
             left join {{ source('int_service_supply', 'suppliers') }} as sup on orders.supplier_id = sup.id
             left outer join {{ ref('addresses') }} sa on sa.address_id = sup.address_id
             left outer join {{ ref('prep_countries') }} c on c.country_id = sa.country_id
    where li.line_item_type = 'part'
      and sourced_at >= '2020-01-01'
      and li.technology_name = 'CNC'
),
     line_item_level_emissions as (
-- Line Item data is combined with greenhubs tables of constants for emissions linked to material procurement and part manufacturing
         select order_uuid,
                -- The 0.33 constant has units of kwh/cm3, the emissions factor units of g-CO2/kwh
                round(li_total_removed_volume_cm3 * 0.33 * kwh.emissions, 0) as li_manufacturing_co2_emissions_g,
                -- The procurement emissions factor is units of g-CO2/kg of material.
                material_density_g_cm3 * line_item_total_bounding_box_volume_cm3 * mat.emissions * 0.001 as li_procurement_co2_emissions_g
         from base_query as li
                  left join {{ ref ('seed_greenhubs_electricity') }} kwh on li.country = kwh.country
                  left join {{ ref ('seed_greenhubs_material') }} mat
                            on li.country = mat.country and li.material = mat.material
     )
select supply.order_uuid,
       min(logistics.logistics_co2_emissions_g) as logistics_co2_emissions_g, -- Already aggregated before
       min(logistics.travel_distance_km) as travel_distance_km, -- Already aggregated before
       sum(li_manufacturing_co2_emissions_g) as manufacturing_co2_emissions_g,
       sum(li_procurement_co2_emissions_g)   as procurement_co2_emissions_g
from line_item_level_emissions as supply
left join {{ source('int_greenhubs', 'co2_emissions') }} as logistics on supply.order_uuid = logistics.order_uuid
where li_manufacturing_co2_emissions_g > 0
group by 1