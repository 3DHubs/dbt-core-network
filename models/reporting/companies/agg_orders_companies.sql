-- Adding MQL from contacts
with mql as (
    select hubspot_company_id, 
               first_value(became_mql_at)
           over ( 
               partition by hubspot_company_id order by (became_mql_at is null)::int, became_mql_at asc rows between unbounded preceding and unbounded following) as became_mql_at
    from {{ ref('dim_contacts') }}
    where hubspot_company_id is not null
)

select
agg.hubspot_company_id,
-- Lifecycle Fields
min(became_mql_at)                  as became_mql_at_company,
min(became_opportunity_at_company)  as became_opportunity_at_company,
min(became_customer_at_company)     as became_customer_at_company,
min(serie_two_order_created_at_company) as serie_two_order_created_at_company,
min(serie_two_order_closed_at_company) as serie_two_order_closed_at_company,
min(serie_three_order_created_at_company) as serie_three_order_created_at_company,
min(serie_three_order_closed_at_company) as serie_three_order_closed_at_company,
min(recent_order_created_at_company) as recent_order_created_at_company,
min(second_order_closed_at_company) as second_order_closed_at_company,
min(recent_closed_order_at_company) as recent_closed_order_at_company,
-- Counts
min(number_of_submitted_orders_company) as number_of_submitted_orders_company,
min(number_of_closed_orders_company)    as number_of_closed_orders_company,
-- Financial Totals
min(closed_sales_usd_company)                      as closed_sales_usd_company,
min(closed_sales_usd_new_customer_company)         as closed_sales_usd_new_customer_company,
min(total_precalc_margin_usd_new_customer_company) as total_precalc_margin_usd_new_customer_company,
-- First Values
min(first_submitted_order_technology_company)      as first_submitted_order_technology_company,
min(first_closed_order_technology_company)         as first_closed_order_technology_company

from {{ ref('agg_orders') }} agg
left join mql on mql.hubspot_company_id = agg.hubspot_company_id
group by agg.hubspot_company_id