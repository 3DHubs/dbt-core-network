{% macro grant_select_on_schemas(schemas, group) %}
  {% for schema in schemas %}
    grant usage on schema {{ schema }} to group {{ group }};
    grant select on all tables in schema {{ schema }} to group {{ group }};
    alter default privileges in schema {{ schema }}
        grant select on tables to group {{ group }};
  {% endfor %}
{% endmacro %}