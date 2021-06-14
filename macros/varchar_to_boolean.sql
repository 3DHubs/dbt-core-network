{% macro varchar_to_boolean(column_name) %}

    decode({{ column_name }}, 'true', True, 'false', False) as {{ column_name }}

{% endmacro %}