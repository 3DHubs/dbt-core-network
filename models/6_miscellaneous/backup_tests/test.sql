{%- set default_schema = target.schema -%}

{%- if default_schema == 'dbt_core'-%}
    select  'production' as schema_last_run
{%- else -%}
    select  'personal' as schema_last_run
{%- endif -%}