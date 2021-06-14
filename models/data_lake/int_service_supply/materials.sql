select
    material_id,
    slug,
    technology_id,
    material_type_id,
    name,
    nullif(short_description, '-') as short_description,
    nullif(long_description, '-') as long_description,
    order_within_technology,
    nullif(tagline, '-') as tagline,
    price_indication,
    slug_scope,
    header_image_id,
    {{ varchar_to_boolean('show_in_material_pages') }}
from int_service_supply.materials