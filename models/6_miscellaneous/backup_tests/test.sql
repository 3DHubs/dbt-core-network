
{%- if target.name == 'dev' -%}
    select  'production' as schema_last_run
{%- else -%}
    select  'personal' as schema_last_run
{%- endif -%}