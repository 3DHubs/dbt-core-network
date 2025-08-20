/*
service-supply discerns three types of events:
- record_order_event()
- record_quote_event()
- record_line_item_event()

This model attempts to standardize some of the events and translate the events into ID (integer) values. Reason behind this it should be easier to pull and process event data this way: you can simply filter on a set of event_ids.

Note 1: on 2021-08-20 we (Diego and Bart) discussed an approach to quicken the data processing of this model and its dependencies by only including events that are currently relevant for children (downstream dependent data models). We changed the methodology to only process that data. If you ever want to change this scope, e.g. add more event types, you'll need to issue a `dbt run --models <this_model> --full-refresh`. Be mindful of the large amount of events. You may want to compile the SQL and do the full refresh in Redshift manually by splitting the data into partitions using the `created` field.
*/

{{
    config(
        materialized = 'incremental',
        sort = ['order_uuid', 'std_event_id'],
        dist = 'created',
        tags=["multirefresh"]
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
from {{ ref('prep_order_history_events') }}

where not regexp_like(lower(description), '(aftership|shippo)')
    and regexp_like(lower(description), 'completed|canceled an auction|dispute') --todo-migration-test: replaced ~ 
    
    {% if is_incremental() %}

        and created > (select max(created) from {{ this }} )

    {% endif %}
)

select
    *,
    case
        --todo-migration-test: replaced ~ 
        -- order events 1XX 
        when regexp_like(description, 'created a new order') then 100 
        when regexp_like(description, 'created a new reorder') then 101
        when regexp_like(description, 'completed') then 102
        when regexp_like(description, 'order accepted') then 103
        when regexp_like(description, 'order cancel(l?)ed') then 104
        when regexp_like(description, 'dispute') then 105
    
        when regexp_like(description, 'in_transit') then 110
        when regexp_like(description, 'delivered') then 111
        when regexp_like(description, 'changed order status to "shipped" on package') then 112
        when regexp_like(description, 'updated order expected shipping date') then 113
        when regexp_like(description, 'moved order to "shipped_to_warehouse"(.*on|\.)') then 114
        when regexp_like(description, 'moved order to "shipped"(.*on|\.)') then 115
        when regexp_like(description, 'partially shipped order') then 116
    
        -- quote events 2XX
        when regexp_like(description, 'created a new quote') then 201
        when regexp_like(description, 'changing default order quote') then 202
        when regexp_like(description, '(quote).*(deleted)') then 203
    
        when regexp_like(description, 'changed order status to "accepted"') then 204
        when regexp_like(description, 'updated quote with') then 205
        when regexp_like(description, 'cloned quote') then 206
        when regexp_like(description, 'quote technology was updated') then 207
    
        when regexp_like(description, 'accepted a counter-bid') then 208
        when regexp_like(description, 'submitted a bid of type') then 209
    
        when regexp_like(description, 'created an auction') then 220
        when regexp_like(description, 'created an rfq auction') then 221
        when regexp_like(description, 'canceled an auction') then 222
        when regexp_like(description, 'accepted an auction') then 223
        when regexp_like(description, 'submitted an rfq auction response') then 224
    
        when regexp_like(description, 'uploaded a quote attachment') then 230
        when regexp_like(description, 'deleted a quote attachment') then 231
    
        when regexp_like(description, 'started payment process for quote') then 240
        when regexp_like(description, 'processed pay later for') then 241
        when regexp_like(description, 'processed payment') then 242
    
        when regexp_like(description, 'requested a refund') then 250
        when regexp_like(description, 'refunded the order') then 251
    
        when regexp_like(description, 'archived quote') then 260
    
        when regexp_like(description, 'docusign signed quote received') then 270
    
        -- line item events 3XX
        when regexp_like(description, 'created.*shipping line item') then 310
        when regexp_like(description, 'deleted shipping line item') then 311
    
        when regexp_like(description, 'added line item') then 300
        when regexp_like(description, 'created a new line item') then 301
        when regexp_like(description, 'updated.*line item') then 302
        when regexp_like(description, 'uploaded a line item attachment') then 303
        when regexp_like(description, 'deleted a line item attachment') then 304
        when regexp_like(description, '(deleted).*(line item)') then 305
    
        -- misc.  9XX
        when regexp_like(description, 'added first-time-offer') then 900
    
        when regexp_like(description, 'cloned invoice') then 910
        when regexp_like(description, 'updated invoice') then 911
    
        when regexp_like(description, 'created purchase order') then 920
        when regexp_like(description, 'cloned purchase_order') then 921
        when regexp_like(description, 'created a new purchase order to reimburse supplier') then 922
        when regexp_like(description, 'voided original purchase order to reimburse supplier') then 923
        when regexp_like(description, 'updated purchase order with no action') then 924
        when regexp_like(description, 'cocked purchase order') then 925
    
        when regexp_like(description, 'created a hubspot deal') then 930

        --todo-migration-test: replaced ~ 
        else -9 -- unknown/undefined event

    end as std_event_id

from events
