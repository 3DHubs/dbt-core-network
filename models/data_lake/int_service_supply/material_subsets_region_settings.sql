{% set boolean_fields = [
    "excluded_in_eu",
    "excluded_in_us"
    ]
%}

select
    id,
    material_subset_id,
    order_in_eu, 
    order_in_us,
    {% for boolean_field in boolean_fields %}
        {{ varchar_to_boolean(boolean_field) }}
        {% if not loop.last %},{% endif %}
    {% endfor %}
from {{ source('int_service_supply', 'material_subsets_region_settings') }}