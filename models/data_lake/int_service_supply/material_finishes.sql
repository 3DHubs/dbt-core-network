select id,
       name,
       slug,
       {{ varchar_to_boolean('has_ral_and_pantone_colors') }},
       description,
       header_image_id,
       machining_marks,
       requirements,
       surface_roughness,
       tolerances,
       cosmetic_type
from int_service_supply.material_finishes