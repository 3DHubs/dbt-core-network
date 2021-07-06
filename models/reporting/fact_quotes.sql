select
    cnc_orders.uuid as order_uuid,
    cnc_orders.created as order_created_date,
    cnc_orders.promised_shipping_date as order_promised_shipping_date,
    cnc_orders.completed_at as order_completed_date,
    cnc_orders.expected_shipping_date as order_expected_ship_date,
    cnc_orders.shipped_at as order_shipped_date,
    cnc_orders.delivered_at as order_delivered_date,
    cnc_orders.billing_request_id as order_billing_request_id,
    cnc_orders.cancellation_reason_id as order_cancellation_reason_id,
    cnc_orders.hubspot_deal_id as order_hubspot_deal_id,
    cnc_orders.hub_id as order_hub_id,
    cnc_orders.quote_uuid as order_quote_uuid,
    cnc_orders.is_admin as order_is_admin,
    cnc_orders.is_automated_shipping_available as order_is_auto_shipping_available,
    cnc_orders.is_strategic as order_is_strategic,
    cnc_orders.session_id as order_session_id,
    cnc_orders.status as order_status,
    cnc_orders.user_id as order_user_id,
    cnc_orders.legacy_order_id as order_legacy_id,
    cnc_orders.accepted_at as order_accepted_date,
    cnc_orders.number as document_number,
    -- Shipping and Geo
    cnc_order_quotes.shipping_address_id,
    -- Required fields from order_quotes for order
    cnc_order_quotes.document_number as order_quote_document_number,
    cnc_order_quotes.currency_code as order_currency_code_sold,
    cnc_order_quotes.price_multiplier as order_quote_price_multiplier,
    cnc_order_quotes.type as order_quote_type,
    cnc_order_quotes.status as order_quote_status,
    cnc_order_quotes.submitted_at as order_submitted_date,
    cnc_order_quotes.finalized_at as order_quote_finalized_date,
    cnc_order_quotes.shipping_address_id as order_quote_shipping_address_id,
    cnc_order_quotes.lead_time as order_quote_lead_time,
    cnc_order_quotes.created as order_quote_created_date,
    cnc_order_quotes.signed_quote_uuid,
    cnc_order_quotes.tax_category_id,
    cnc_order_quotes.is_cross_docking,
    cnc_order_quotes.cross_docking_added_lead_time,
    cnc_order_quotes.requires_local_production,
    'supply_deals' as _data_source
from {{ ref('cnc_orders') }}
            -- Data Lake joins
            -- This brings in the "active" quote for an order (subsequent quotes are not included here)
            -- In case of cart quotes, it will be the latest version, in case of submitted or further
-- down the pipeline, it will be the "locked quote"
left join {{ ref('cnc_order_quotes') }}
    on cnc_order_quotes.uuid = cnc_orders.quote_uuid
