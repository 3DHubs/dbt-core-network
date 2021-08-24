----------------------------------------------------------------
-- FINANCIAL DATA at ORDER LEVEL
----------------------------------------------------------------

-- Sources:
-- 1. Stripe Transactions, from Supply.
-- 2. Refunds, from Supply.
-- 3. Docusign Requests
-- 4. Netsuite, from Netsuite integration (Coming Soon)

with stripe_transactions as (
    select t.quote_uuid,
           -- Main Fields
           max(t.created)                                                                     as stripe_transaction_created_at,
           max(case when t.status <> 'failed' and t.type = 'payment' then payment_method end) as stripe_payment_method,
           min(case
                   when (t.status = 'successful' or t.status = 'refunded')
                       and t.type = 'payment'
                       then 1 end)::bool                                                      as stripe_is_successful_payment,
           -- Secondary Fields (Not leveraged but might be useful later)
           sum((fee_amount::float / 100)::decimal(15, 2))                                     as fee_amount,
           min(case when t.status = 'successful' and t.type = 'refund' then 1 end)::bool      as is_successful_refund,
           min(case when t.status = 'failed' and t.type = 'payment' then 1 end)::bool         as is_failed_payment,
           sum(case when t.status = 'failed' and t.type = 'payment' then 1 end)               as num_failed_payments
    from {{ ref('transactions') }} as t
    where status != 'new' -- 'Pending' transactions discarded
    group by 1
)

select o.uuid                         as order_uuid,
       t.stripe_transaction_created_at,
       t.stripe_is_successful_payment,
       case
           when q.signed_quote_uuid is not null then true
           when q.customer_purchase_order_uuid is not null then true
           when t.stripe_is_successful_payment is true then true
           else false end                is_auto_payment,
       case
           when t.stripe_transaction_created_at::date = dealstage.order_closed_at::date
               and t.stripe_payment_method <> 'stripe_source_sofort'
               and t.stripe_is_successful_payment is true then true
           else false end             as is_instant_payment,
       case
           when d.envelope_id is not null then '2. Docusign'
           when q.customer_purchase_order_uuid is not null then '3. Purchase order upload'
           when q.signed_quote_uuid is not null then '4. Signed quote upload'
           when t.stripe_payment_method is not null then '1. Stripe payment'
           else '5. Manual Net30' end as payment_method

       -- Note: field "is_instant_payment" is defined in core orders (a.k.a cube deals)
       --       as it requires of the "is_closed_won" field coming from other sources.

from {{ ref('cnc_orders') }} as o
    left join {{ ref('cnc_order_quotes') }} as q
    on o.quote_uuid = q.uuid
    left join stripe_transactions as t
    on o.quote_uuid = t.quote_uuid
    left join {{ ref ('quote_docusign_requests') }} as d
    on o.quote_uuid = d.quote_uuid
    left join {{ ref('stg_orders_dealstage') }} as dealstage on o.uuid = dealstage.order_uuid
