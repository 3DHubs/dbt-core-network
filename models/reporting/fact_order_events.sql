/*
service-supply discerns three types of events:
- record_order_event()
- record_quote_event()
- record_line_item_event()

This model attempts to standardize some of the events and translate the events into ID (integer) values. Reason behind this it should be easier to pull and process event data this way: you can simply filter on a set of event_ids.
*/

{{
    config(
        materialized = 'incremental',
        sort = ['order_uuid', 'std_event_id'],
        dist = 'created'
    )
}}

with events as (
select
    id,
    created,
    order_uuid,
    quote_uuid,
    line_item_uuid,
    user_id,
    anonymous_id,
    lower(description) as description
from {{ ref('order_history_events') }}

{% if is_incremental() %}

    where created > (select max(created) from {{ this }} )

    {% endif %}

where lower(description) !~ '(aftership|shippo)'
)

select
    *,
    case
    -- order events 1XX
            when description ~ 'created a new order' then 100
            when description ~ 'created a new reorder' then 101
            when description ~ 'completed' then 102
            when description ~ 'order accepted' then 103
            when description ~ 'order cancel(l?)ed' then 104
            when description ~ 'dispute' then 105

            when description ~ 'in_transit' then 110
            when description ~ 'delivered' then 111
            when description ~ 'changed order status to "shipped" on package' then 112
            when description ~ 'updated order expected shipping date' then 113
    -- Excluding shipping updates from Shippo (see shipping_labels.py)
            when description ~ 'moved order to "shipped_to_warehouse"(.*on|\.)' then 114
    -- Excluding shipping updates from Shippo (see shipping_labels.py)
            when description ~ 'moved order to "shipped"(.*on|\.)' then 115
            when description ~ 'partially shipped order' then 116


    -- quote events 2XX
            when description ~ 'created a new quote' then 201
            when description ~ 'changing default order quote' then 202
            when description ~ '(quote).*(deleted)' then 203

            when description ~ 'changed order status to "accepted"' then 204
            when description ~ 'updated quote with' then 205
            when description ~ 'cloned quote' then 206
            when description ~ 'quote technology was updated' then 207

            when description ~ 'accepted a counter-bid' then 208
            when description ~ 'submitted a bid of type' then 209

            when description ~ 'created an auction' then 220
            when description ~ 'created an rfq auction' then 221
            when description ~ 'canceled an auction' then 222
            when description ~ 'accepted an auction' then 223
            when description ~ 'submitted an rfq auction response' then 224

            when description ~ 'uploaded a quote attachment' then 230
            when description ~ 'deleted a quote attachment' then 231

            when description ~ 'started payment process for quote' then 240
            when description ~ 'processed pay later for' then 241
            when description ~ 'processed payment' then 242

            when description ~ 'requested a refund' then 250
            when description ~ 'refunded the order' then 251

            when description ~ 'archived quote' then 260

            when description ~ 'docusign signed quote received' then 270

    -- line item events 3XX
            when description ~ 'created.*shipping line item' then 310
            when description ~ 'deleted shipping line item' then 311

            when description ~ 'added line item' then 300
            when description ~ 'created a new line item' then 301
            when description ~ 'updated.*line item' then 302
            when description ~ 'uploaded a line item attachment' then 303
            when description ~ 'deleted a line item attachment' then 304
            when description ~ '(deleted).*(line item)' then 305

    -- misc.  9XX
            when description ~ 'added first-time-offer' then 900

            when description ~ 'cloned invoice' then 910
            when description ~ 'updated invoice' then 911

            when description ~ 'created purchase order' then 920
            when description ~ 'cloned purchase_order' then 921
            when description ~ 'created a new purchase order to reimburse supplier' then 922
            when description ~ 'voided original purchase order to reimburse supplier' then 923
            when description ~ 'updated purchase order with no action' then 924
            when description ~ 'cocked purchase order' then 925

            when description ~ 'created a hubspot deal' then 930

            else -9 -- unknown/undefined event

    end as std_event_id

from events
