{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}

with supply_orders as (
    select distinct so.uuid, quote_uuid
    from {{ ref('cnc_orders') }} as so
    where exists(
                  select *
                  from {{ ref('cnc_order_quotes') }} as soq
                  where status != 'cart'
                    and so.uuid = soq.order_uuid
              )
)
select so.uuid                                                  as order_uuid,
       l.quote_uuid,
       discount_code_id,
       l.discount_id,
       d.discount_factor,
       coalesce(trunc(-l.auto_price_amount *1.0 / 100.00,2), 0) as discount_amount_local_currency,
       trunc((discount_amount_local_currency / rates.rate), 2)   as discount_amount_usd,
       d.title,
       dc.description,
       dc.code,
       u.first_name + ' ' + u.last_name created_by,
       d.currency_code,
       is_hidden
from {{ ref('line_items') }} l
         inner join {{ ref('cnc_order_quotes') }} coq on coq.uuid = l.quote_uuid
         inner join supply_orders so on so.quote_uuid = l.quote_uuid
         inner join {{ ref('discounts') }} d on d.id = l.discount_id
         left join  {{ ref('discount_codes') }} dc on dc.id = l.discount_code_id
         left join  {{ ref('users') }} u on u.user_id = dc.author_id
         left join  {{ source('data_lake', 'exchange_rate_spot_daily') }} as rates
                   on rates.currency_code_to = coq.currency_code
                     and trunc(l.created) = trunc(rates.date)
where l.type='discount'
order by l.created