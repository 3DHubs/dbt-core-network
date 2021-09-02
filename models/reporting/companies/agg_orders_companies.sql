select
hubspot_company_id,
-- Lifecycle Fields
min(became_opportunity_at_company)  as became_opportunity_at_company,
min(became_customer_at_company)     as became_customer_at_company,
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

from {{ ref('agg_orders') }}
group by hubspot_company_id