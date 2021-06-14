{% set boolean_fields = [
    "is_order_level",
    "is_quote_level",
    "is_line_item_level"
    ]
%}

select created,
       updated,
       deleted,
       id,
       name,
       description,
       machine_name,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}
from int_service_supply.question_groups