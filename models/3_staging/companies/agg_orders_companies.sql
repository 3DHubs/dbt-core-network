-- Adding MQL from contacts
select distinct hubspot_company_id,
-- Lifecycle Fields
became_opportunity_at_company,
became_customer_at_company,
serie_two_order_created_at_company,
serie_two_order_closed_at_company,
serie_three_order_created_at_company,
serie_three_order_closed_at_company,
recent_order_created_at_company,
second_order_closed_at_company,
recent_closed_order_at_company,
-- Counts
number_of_submitted_orders_company,
number_of_closed_orders_company,
-- Financial Totals
closed_sales_usd_company,
closed_sales_usd_new_customer_company,
total_precalc_margin_usd_new_customer_company,
-- First Values
first_submitted_order_technology_company,
first_closed_order_technology_company,
is_integration_company

from {{ ref('agg_orders') }} agg
where hubspot_company_id is not null
