
{% macro unload_backups_to_s3(source_name, table_name, ref_bool=True) -%}
    -- Code adjustment ensures that code only runs in prod not in dev
    -- Source name examples: dbt_prod_reporting or datalake
    -- Table name is the table you would like to back up
    -- ref inidicates if the table comes from a source or reference

    {% set now = modules.datetime.datetime.now() %}
    {% set source_name_str = source_name %}
    {% set table_name_str = table_name %}

    {% if target.schema == 'dbt_prod' | as_bool %}

        {% if ref_bool == True %}

            unload ('SELECT * FROM {{ref(table_name_str)}}') 
            to 's3://hubs-prod-analytics/backups/{{table_name_str}}/{{ now }}/'
            iam_role 'arn:aws:iam::256629611817:role/redshift-spectrum'
            parquet
            maxfilesize 100 mb

        {% else %}

            unload ('SELECT * FROM {{ source(source_name_str, table_name_str) }}') 
            to 's3://hubs-prod-analytics/backups/{{table_name_str}}/{{ now }}/'
            iam_role 'arn:aws:iam::256629611817:role/redshift-spectrum'
            parquet
            maxfilesize 100 mb

        {% endif %}

    {% else %}
        select 1
    {% endif %}
{%- endmacro %}


{% macro back_up_to_datalake(source_name, table_name, ref_bool=True) -%}
    -- Code adjustment ensures that code only runs in prod not in dev
    -- Source name examples: dbt_prod_reporting or datalake
    -- Table name is the table you would like to back up
    -- ref inidicates if the table comes from a source or reference

    {% if target.schema == 'dbt_prod' | as_bool %}

        {% set source_name_str = source_name %}
        {% set table_name_str = table_name %}
        {% set backup_table_name_str = 'backup_' ~ table_name_str %}

        {% if ref_bool == True %}

            drop table if exists {{ source('dbt_backups', backup_table_name_str) }};
            create table {{ source('dbt_backups', backup_table_name_str) }} as
            select getdate() as backup_date, * from {{ref(table_name_str)}};

        {% else %}

            drop table if exists {{ source('dbt_backups', backup_table_name_str) }};
            create table {{ source('dbt_backups', backup_table_name_str) }} as
            select getdate() as backup_date, * from {{ source(source_name_str, table_name_str) }};

        {% endif %}

    {% else %}

        select 1
        
    {% endif %}

{%- endmacro %}
