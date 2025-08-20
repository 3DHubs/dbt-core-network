-- This model queries directly from int_service_supply.cnc_orders as conveniently renames it
-- as supply_orders as cnc_orders is not exclusive to the CNC technology. Furthermore in this model
-- by implementing an inner join with line items we filter out empty orders or carts (orders at their earliest stage).

{{ config(  materialized='table',
            tags=["multirefresh"]
    ) }}

select orders.created,
       orders.updated,
       orders.billing_request_id,
       orders.hubspot_deal_id,
       orders.uuid,
       orders.reorder_original_order_uuid,
       orders.reorder_with_same_mp_order_uuid,
       orders.user_id,
       orders.quote_uuid,
       orders.status,
       orders.delivered_at,
       orders.shipped_at,
       orders.expected_shipping_date, -- will be used for additional otr indicator
       orders.legacy_order_id,
       orders.completed_at,
       orders.support_ticket_id,
       orders.cancellation_reason_id,
       orders.number,
       orders.in_production_at,
       orders.promised_shipping_date, -- is C-OTR target date
       orders.accepted_at,
       orders.is_eligible_for_restriction,
       orders.order_change_request_status,
       orders.order_change_request_freshdesk_ticket_id

from {{ ref('orders') }} as orders
        left join {{ ref('prep_supply_integration') }} as pse on orders.uuid = pse.order_uuid 
        left join {{ ref('prep_users') }} as users on orders.user_id = users.user_id 
        left join {{ ref('anonymous_user_carts') }} auc on orders.uuid = auc.order_uuid and (auc.anonymous_user_email = 'test@hubs.com' or regexp_like(auc.anonymous_user_email, '@pthubs.com')) --todo-migration-test: replaced ~ for regexp_like
-- Filter: only orders with line items on the main quote, this removes empty carts.
where exists (
    select 1 from {{ ref('gold_line_items') }} as li
    where orders.quote_uuid = li.quote_uuid
)
-- Filters: external orders created through the PAPI integration
and not pse.is_test --todo-migration-test: replaced not true for not {field}

-- Filters: to exclude internal test traffic
-- and (users.is_test is not true or sfr.order_uuid is not null)

-- Filters: to exclude internal anonymous tests with the email test@hubs.com
and auc.anonymous_user_email is null