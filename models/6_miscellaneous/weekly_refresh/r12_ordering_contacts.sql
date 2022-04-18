{% set max_counter = 24 %}

{% for _ in range(1, max_counter+1) %}
	
   {% if loop.index < max_counter %}
    select date_add('month',1-{{loop.index}},date_trunc('month',getdate())) as date,
    count(distinct fo.hubspot_company_id) as companies,
    count(distinct fo.hubspot_contact_id) as contacts
    from {{ ref('fact_orders') }} fo
    where date_trunc('month',closed_at) >= date_add('month',-10-{{loop.index}},date_trunc('month',getdate()))
    and date_trunc('month',closed_at) < date_add('month',2-{{loop.index}},date_trunc('month',getdate()))
    and fo.is_strategic
	UNION ALL
   {% elif loop.index == max_counter %}
    select date_add('month',1-{{loop.index}},date_trunc('month',getdate())) as date,
    count(distinct fo.hubspot_company_id) as companies,
    count(distinct fo.hubspot_contact_id) as contacts
    from {{ ref('fact_orders') }} fo
    where date_trunc('month',closed_at) >= date_add('month',-10-{{loop.index}},date_trunc('month',getdate()))
    and date_trunc('month',closed_at) < date_add('month',2-{{loop.index}},date_trunc('month',getdate()))
    and fo.is_strategic
   {% endif %}

{% endfor %}