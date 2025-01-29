select
    hubspot_company_id,
    created_at,
    name,
    number_of_employees,
    industry,
    industry_mapped,
    founded_year,
    is_funded,
    is_strategic,
    is_integration_company,
    hubspot_owner_name,
    hubspot_network_sales_specialist_name,
    country_iso2,
    country_name,
    continent,
    market,
    region,
    sub_region,
    city,
    city_latitude,
    city_longitude,
    became_customer_at, --First date company contact placed an order
    number_of_submitted_orders, --Total quote requests
    number_of_closed_orders, --Total place orders
    closed_sales_usd, --Lifetime revenue
    first_closed_order_technology, --First used service
    number_of_inside_mqls, --# of contacts that placed an upload
    number_of_inside_opportunities, --# of contacts that placed a quote request
    number_of_inside_customers, --# of contacts that placed an order
    alive_probability, --How alive the customer is in HUBS at the moment
    potential_tier --Algorithm that determines after first order what the potential is in HUBS vs other customers.
    from {{ ref('dim_companies') }}
