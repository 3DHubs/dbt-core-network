{% set boolean_fields = [
    "is_available_in_auctions",
    "is_available_to_suppliers",
    "has_ral_and_pantone_colors",
    "show_in_material_pages"
    ]
%}

select material_subset_id,
       material_id,
       process_id,
       composite_id,
       name,
       short_description,
       order_within_process,
       image_id,
       slug,
       slug_scope,
       density,
       alternative_name,
       anodizing_compatibility,
       common_applications,
       corrosion_resistance,
       electrical_resistivity,
       elongation_at_break,
       engineering_comment,
       esd_safety,
       hardness,
       magnetism,
       maximum_service_temperature,
       post_processing,
       thermal_conductivity,
       thermal_expansion_coefficent,
       ultimate_tensile_strength,
       uv_resistance,
       weldability,
       yield_strength,
       youngs_modulus,
       datasheet_id,
       flexural_strength,
       glass_transition_temperature,
       heat_deflection,
       izod_impact,
       melting_point,
              {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}
from int_service_supply.material_subsets