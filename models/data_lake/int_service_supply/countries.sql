{% set boolean_fields = [
    "is_in_eu",
    "has_payment_embargo",
    "is_in_efta"
    ]
%}

select created,
       updated,
       deleted,
       country_id,
       name,
       alpha2_code,
       continent,
       currency_code,
       coordinates,
       lat,
       lon,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}
from int_service_supply.countries