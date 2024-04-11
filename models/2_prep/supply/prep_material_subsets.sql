with distinct_material_subset_region_settings as (
    select
        material_subset_id,
        excluded_in_us,
        excluded_in_eu
    from {{ source('int_service_supply', 'material_subsets_region_settings') }}
    group by 1,2,3
)


{% set boolean_fields = [
        "is_available_in_auctions",
        "is_available_to_suppliers",
        "has_ral_and_pantone_colors",
        "show_in_material_pages"
    ]
%}

select 
    ms.alternative_name,
    ms.anodizing_compatibility,
    ms.common_applications,
    ms.composite_id,
    ms.corrosion_resistance,
    ms.datasheet_id,
    ms.density,
    ms.electrical_resistivity,
    ms.elongation_at_break,
    ms.engineering_comment,
    ms.esd_safety,
    ms.flexural_strength,
    ms.glass_transition_temperature,
    ms.hardness,
    ms.heat_deflection,
    ms.image_id,
    ms.izod_impact,
    ms.magnetism,
    ms.material_id,
    ms.material_subset_id,
    ms.maximum_service_temperature,
    ms.melting_point,
    ms.name,
    ms.order_within_process,
    ms.post_processing,
    ms.process_id,
    ms.short_description,
    ms.slug_scope,
    ms.slug,
    ms.thermal_conductivity,
    ms.thermal_expansion_coefficent,
    ms.ultimate_tensile_strength,
    ms.uv_resistance,
    ms.weldability,
    ms.yield_strength,
    ms.youngs_modulus,

    {% for boolean_field in boolean_fields %}
        {{ varchar_to_boolean(boolean_field) }}
        {% if not loop.last %}{% endif %},
    {% endfor %}

    coalesce(decode(msrs.excluded_in_eu, 'true', True, 'false', False), False) as material_excluded_in_eu,
    coalesce(decode(msrs.excluded_in_us, 'true', True, 'false', False), False) as material_excluded_in_us

from {{ source('int_service_supply', 'material_subsets') }} as ms
left join distinct_material_subset_region_settings as msrs on ms.material_subset_id = msrs.material_subset_id