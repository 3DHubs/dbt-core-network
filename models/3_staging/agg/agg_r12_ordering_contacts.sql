{% set max_counter = 24 %}

{% for _ in range(1, max_counter+1) %}
    
    {% if loop.index < max_counter %}
    select dateadd(month, 1-{{ loop.index }}, date_trunc('month', current_date)) as date, --todo-migration-test dateadd and current_date
    count(distinct fo.hubspot_company_id) as companies,
    count(distinct fo.hubspot_contact_id) as contacts
    from {{ ref('fact_orders') }} fo
    where date_trunc('month', closed_at) >= dateadd(month, -10-{{ loop.index }}, date_trunc('month', current_date)) --todo-migration-test dateadd and current_date
        and date_trunc('month', closed_at) < dateadd(month, 2-{{ loop.index }}, date_trunc('month', current_date)) --todo-migration-test dateadd and current_date
        and fo.is_strategic
    union all
    {% elif loop.index == max_counter %}
    select dateadd(month, 1-{{ loop.index }}, date_trunc('month', current_date)) as date, --todo-migration-test dateadd and current_date
    count(distinct fo.hubspot_company_id) as companies,
    count(distinct fo.hubspot_contact_id) as contacts
    from {{ ref('fact_orders') }} fo
    where date_trunc('month', closed_at) >= dateadd(month, -10-{{ loop.index }}, date_trunc('month', current_date)) --todo-migration-test dateadd and current_date
        and date_trunc('month', closed_at) < dateadd(month, 2-{{ loop.index }}, date_trunc('month', current_date)) --todo-migration-test dateadd and current_date
    and fo.is_strategic
   {% endif %}

{% endfor %}