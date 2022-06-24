{% macro unload_backups_to_s3(table_name) -%}

	{% set now = modules.datetime.datetime.now() %}
	{% set table_name_str = table_name %}
	unload ('SELECT * FROM {{ref(table_name_str)}}') 
	to 's3://hubs-prod-analytics/backups/{{table_name_str}}/{{ now }}/'
	iam_role 'arn:aws:iam::256629611817:role/redshift-spectrum'
	parquet
	maxfilesize 100 mb
    
{%- endmacro %}


{% macro unload_datalake_backups_to_s3(table_name) -%}

	{% set now = modules.datetime.datetime.now() %}
	{% set table_name_str = table_name %}
	{% set source_table = 'data_lake' %}
	unload ('SELECT * FROM {{ source(source_table, table_name_str) }}') 
	to 's3://hubs-prod-analytics/backups/{{table_name_str}}/{{ now }}/'
	iam_role 'arn:aws:iam::256629611817:role/redshift-spectrum'
	parquet
	maxfilesize 100 mb
    
{%- endmacro %}