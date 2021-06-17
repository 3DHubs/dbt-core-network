{{
    config(
        post_hook = "analyze {{ this }}"
    )
}}

-- Originated from fact_invoices model
select quote_uuid,
       sum(case
               when (li.type = 'shipping' or (li.type = 'custom' and lower(title) like '%shipping%')) and
                   not (li.type = 'shipping' and lower(title) like '%refund%') then nvl(price_amount, 0)
               else 0 end)                                                       as order_shipping_revenue,
       sum(case
               when li.type = 'shipping' and lower(title) like '%refund%' then price_amount
               else 0 end)                                                       as order_special_revenue,
       sum(case
               when li.type = 'part' and li.technology_id = 3
                   then round((coalesce(price_amount, unit_price_amount * quantity::double precision,
                                       auto_price_amount::double precision *
                                       coalesce(soq.price_multiplier, li.price_multiplier)) +
                               coalesce(tooling_price_amount, auto_tooling_price_amount)),
                               2) -- Tooling costs only apply to IM parts
               when li.type = 'part'
                   then round(coalesce(price_amount, unit_price_amount * quantity::double precision,
                                       auto_price_amount::double precision *
                                       coalesce(soq.price_multiplier, li.price_multiplier)),
                               2) -- Price_amount is the manually overridden price field and auto_price_amount is automatically generated. Only multiply by quantity if price is per unit
               else round(coalesce(price_amount, unit_price_amount * quantity::double precision,
                                   auto_price_amount::double precision),
                           2) -- Price multiplier is only applied for parts
           end)                                                                  as order_parts_revenue,
       sum(case when li.type = 'surcharge' then nvl(price_amount, 0) else 0 end) as order_surcharge

from {{ ref('line_items') }} as li
            left join {{ ref('cnc_order_quotes') }} soq on soq.uuid = li.quote_uuid
group by 1