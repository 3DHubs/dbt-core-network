select id,
       name,
       slug,
       decode(has_ral_and_pantone_colors, 'true', True, 'false', False) as has_ral_and_pantone_colors,
       description,
       header_image_id,
       machining_marks,
       requirements,
       surface_roughness,
       tolerances,
       cosmetic_type
from int_service_supply.material_finishes