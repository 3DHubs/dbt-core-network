select
hubspot_contact_id,
-- Lifecycle Fields
min(became_opportunity_at_contact) as became_opportunity_at_contact,
min(became_customer_at_contact) as became_customer_at_contact,
min(serie_two_order_created_at_contact) as serie_two_order_created_at_contact,
min(serie_two_order_closed_at_contact) as serie_two_order_closed_at_contact,
min(serie_three_order_created_at_contact) as serie_three_order_created_at_contact,
min(serie_three_order_closed_at_contact) as serie_three_order_closed_at_contact,
min(recent_order_created_at_contact) as recent_order_created_at_contact,
min(second_order_closed_at_contact) as second_order_closed_at_contact,
min(recent_closed_order_at_contact) as recent_closed_order_at_contact,
-- Counts
min(number_of_submitted_orders_contact) as number_of_submitted_orders_contact,
min(number_of_closed_orders_contact) as number_of_closed_orders_contact,
-- Financial Totals
min(closed_sales_usd_contact) as closed_sales_usd_contact,
min(closed_sales_usd_new_customer_contact) as closed_sales_usd_new_customer_contact,
min(total_precalc_margin_usd_new_customer_contact) as total_precalc_margin_usd_new_customer_contact,
min(total_precalc_margin_usd_contact_90d) as total_precalc_margin_usd_contact_90d,
min(total_precalc_margin_usd_contact_24m) as total_precalc_margin_usd_contact_24m,
-- First Values
min(first_submitted_order_technology_contact) as first_submitted_order_technology_contact,
min(first_closed_order_technology_contact) as first_closed_order_technology_contact,
min(first_closed_order_process_name_contact) as first_closed_order_process_name_contact,
min(first_submitted_order_country_iso2) as first_submitted_order_country_iso2,
min(first_integration_type_contact) as first_integration_type_contact,
bool_or(is_integration_contact) as is_integration_contact, --due cart logic to allocate a company, there may be true / false combinations happening e.g. contact 479754101. This will just use if contact has true, then true.
-- Averages & Medians
avg(case when number_of_closed_orders_contact > 2 then days_from_previous_closed_order_contact end) as average_days_between_closed_orders_contact,
median(case when number_of_closed_orders_contact > 2 then days_from_previous_closed_order_contact end) as median_days_between_closed_orders_contact

from {{ ref('agg_orders') }}
group by hubspot_contact_id
