{% set boolean_fields = [
    "is_expedited",
    "is_active",
    "is_allowed_to_override_price",
    "is_default"
    ]
%}

select id,
       name,
       min_price_amount,
       currency_code,
       percentage_from_total,
       created,
       updated,
       min_days,
       max_days,
       shipping_leg,
       region,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}

from {{ source('int_service_supply', 'shipping_options') }}