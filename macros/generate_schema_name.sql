{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set user_name = target.user -%}
    {%- set root_schema = 'dbt_' -%}
    {%- set target_project = 'analytics' -%}
    {%- set environment = env_var('DBT_ENVIRONMENT') -%}
    {%- set default_schema = target.schema -%}


    {%- if environment == 'dev' -%}
        {%- set default_schema = root_schema ~ environment ~  '_' ~ target_project ~ '_' ~ user_name ~ '_' ~ custom_schema_name -%}
        {{ default_schema }}


    {%- elif environment == 'prod' -%}
        {%- if custom_schema_name is none -%}
            {{ default_schema }}

        {%- else -%}
             {{ default_schema }}_{{ custom_schema_name }}
        
        {%- endif -%}
        
    {%- else -%}
        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}
