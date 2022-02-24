-- This table queries a subset of columns from int service supply cnc orders,
-- it also transforms binary fields from text to booleans. Int service supply schema
-- is a replica of the production database where our platform data lives.

select created,
       updated,
       deleted,
       id,
       billing_request_id,
       hubspot_deal_id,
       uuid,
       reorder_original_order_uuid,
       user_id,
       quote_uuid,
       session_id,
       status,
       delivered_at,
       shipped_at,
       expected_shipping_date,
       legacy_order_id,
       completed_at,
       support_ticket_id,
       number,
       cancellation_reason_id,
       in_production_at,
       promised_shipping_date,
       accepted_at,
       description,
       shipped_to_warehouse_at,
       -- Boolean Fields
       {{ varchar_to_boolean('is_admin') }},
       {{ varchar_to_boolean('is_strategic') }},
       {{ varchar_to_boolean('is_automated_shipping_available') }},
       {{ varchar_to_boolean('should_create_auction') }},
       {{ varchar_to_boolean('is_eligible_for_restriction') }}

from {{ source('int_service_supply', 'cnc_orders') }}