{{
       config(
              post_hook = "analyze {{ this }}"
       )
}}

select coalesce(max(case when status in ('successful', 'refunded') then created end),
                max(case when status in ('failed', 'new') then created end))          as created_at,
       coalesce(max(case when status in ('successful', 'refunded') then updated end),
                max(case when status in ('failed', 'new') then updated end))          as updated_at,
       max(deleted)                                                                   as deleted_at,
       quote_uuid,
       max(case when status <> 'failed' and type = 'payment' then payment_method end) as payment_method,
       max(amount::float / 100)::decimal(15, 2)                                       as payment_amount,
       currency_code,
       sum((fee_amount::float / 100)::decimal(15, 2))                                 as fee_amount,
       min(case
               when (status = 'successful' or status = 'refunded') and type = 'payment'
                   then 1 end)::bool                                                  as is_successful_payment,
       min(case when status = 'successful' and type = 'refund' then 1 end)::bool      as is_successful_refund,
       min(case when status = 'failed' and type = 'payment' then 1 end)::bool         as is_failed_payment,
       sum(case when status = 'failed' and type = 'payment' then 1 end)               as num_failed_payments
from {{ ref('transactions') }}
where status != 'new' -- 'Pending' transactions discarded
group by quote_uuid, currency_code