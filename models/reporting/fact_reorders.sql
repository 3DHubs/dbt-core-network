with reorders as (select reorder_original_order_uuid,
                         order_uuid,
                         order_created_date,
                         order_quote_lead_time,
                         part_quantity,
                         is_expedited_shipping,
                         quote_subtotal_amount_usd
                  from reporting.cube_deals
                  where reorder_original_order_uuid is not null)
select reorders.order_uuid                                                                                  as reorder_order_uuid,
       reorders.reorder_original_order_uuid,
       reorders.order_created_date                                                                          as reorder_created_date,
       reorders.order_quote_lead_time                                                                       as reorder_lead_time,
       reorders.part_quantity                                                                               as reorder_total_quantity,
       reorders.is_expedited_shipping                                                                       as reorder_is_expedited,
       reorders.quote_subtotal_amount_usd                                                                   as reorder_order_amount_usd,
       cd.order_created_date                                                                                as original_order_created_date,
       cd.order_quote_lead_time                                                                             as original_order_lead_time,
       cd.part_quantity                                                                                     as original_order_total_quantity,
       cd.is_expedited_shipping                                                                             as original_order_is_expedited,
       cd.quote_subtotal_amount_usd                                                                         as original_order_amount_usd,
       -- Stats
       trunc(reorder_created_date) - trunc(original_order_created_date)                                     as reorder_in_days,
       (reorder_order_amount_usd - original_order_amount_usd)::decimal(15, 2)                               as abs_order_amount_change,
       (((reorder_order_amount_usd / original_order_amount_usd) - 1) * 100)::decimal(15, 1)                 as pct_order_amount_change,
       reorder_total_quantity - original_order_total_quantity                                               as abs_order_total_quantity_change,
       (((reorder_total_quantity::float / original_order_total_quantity::float) - 1) * 100)::decimal(15, 1) as
                                                                                                               pct_order_total_quantity_change,
       (original_order_amount_usd / original_order_total_quantity)::decimal(15, 2)                          as original_order_amount_per_unit,
       (reorder_order_amount_usd / reorder_total_quantity)::decimal(15, 2)                                  as reorder_amount_per_unit,
       reorder_is_expedited != original_order_is_expedited                                                  as has_changed_shipping_speed,
       reorder_total_quantity != original_order_total_quantity                                              as has_changed_quantity,
       reorder_lead_time != original_order_lead_time                                                        as has_changed_lead_time
from reorders
         inner join reporting.cube_deals cd on reorders.reorder_original_order_uuid = cd.order_uuid