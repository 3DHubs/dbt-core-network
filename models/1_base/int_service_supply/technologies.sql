{% set boolean_fields = [
        "has_threads",
        "has_tolerances",
        "has_parting_lines",
        "has_rapid_tooling",
    ]
%}

select technology_id,
       slug,
       slug_scope,
       decode(admin_only, 'true', True, 'false', False)         as is_admin_only,
       country_codes,
       standard_tolerance,
       unsupported_country_codes,
       name                                                     as "original_name",
       decode(technology_id,
              1, 'CNC',
              2, '3DP',
              3, 'IM',
              5, 'Urethane Casting',
              6, 'SM',
              7, 'Casting')                                     as name,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}

from {{ source('int_service_supply', 'technologies') }}