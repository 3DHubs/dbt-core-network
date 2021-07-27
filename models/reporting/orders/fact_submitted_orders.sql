select 

* 

--Renamed Fields
-- has_quote_crated_by_admin, Looker: has_any_quote_created_by_admin

-- Hidden Fields (Not used directly)
-- active_po_document number (Used for Inspection Button)
-- active_po_amount_usd (Used in other fields e.g. pre-calculated margin active)
-- became_customer_date (Used in cohorts)
-- became_opportunity_date (Used in Cohorts)
-- client_id (Used for counts measures, should be replaced with contact/company)
-- country_name (currently called countr_deal, name to be thought off)
-- first_time_quote_sent_date (Used in "time to issue a quote")
-- first_time_response_date (Used in "time to first response")
-- 


-- Excluded Dimensions
-- _data_source (Not necessary in this layer)
-- first_deal_is_legacy (No longer aplicable)
-- first_quote_submitted_date_contact_date (Not used)
-- 




from {{ ref('fact_orders') }} as orders
left join {{ ref('agg_orders') }} as agg_orders using(order_uuid) 
where true
and order_quote_status <> 'cart'