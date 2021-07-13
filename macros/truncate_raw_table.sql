{% macro truncate_raw_table(unique_key, partition_list, order_column, target_table) %}

    with all_rows (
        select {{ unique_key }},
               row_number() over (
                   partition by {{ partition_list|join(', ') }}
                   order by {{ order_column }} desc
               ) as row_number

        from {{ target_table }}
    )
    delete from {{ target_table }} as target_table
    where exists (
        select 1 from all_rows where target_table.{{ unique_key }} = all_rows.{{ unique_key }}

    )

{% endmacro %}