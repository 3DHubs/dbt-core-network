
{% if target.name =! 'dbt_prod' | as_bool %}
    select 1 as test
{% else %}
    select 2 as test
{% endif %}