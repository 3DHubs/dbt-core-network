-- This model will be used to filter out test order data in the various objects e.g. fact_sales_orders.
select
       quotes.uuid,
       quotes.order_uuid,
       quotes.document_number,
       case when quotes.created < '2022-10-01' or lower(consumer_purchase_order_number) ~ 'test' or ql.request_id ~ 'test'
       or ql.email ~ 'mailinator' or ql.email ~ 'protolabs' then true else false end is_test,
       decode(is_external, 'true', True, 'false', False)                                      as is_papi_integration,
       case when is_external = 'true' then 'papi'
           when ql.quote_id is not null then 'quicklink' else 'shallowquicklink' end          as integration_type,
       external_orders.consumer_order_id                                                      as integration_order_id,
       ql.request_id                                                                          as integration_quote_id,
       coalesce(external_orders.consumer_order_number,qt.quote_number)                        as integration_order_number,
       external_orders.consumer_purchase_order_number                                         as integration_purchase_order_number,
       coalesce(external_orders.consumer_order_created_at,ql.created_at)                      as integration_order_created_at,
       external_orders.consumer_ship_by                                                       as integration_order_ship_by_at,
       coalesce(qt.user_id,ql.user_id)                                                        as integration_user_id,
       replace(qt.utm_content,'content=','')                                                  as integration_utm_content

from {{ source('int_service_supply', 'cnc_order_quotes') }}  as quotes
       inner join {{ source('int_service_supply', 'cnc_orders') }} as orders on orders.quote_uuid = quotes.uuid
       left join {{ source('int_service_supply', 'external_orders') }} as external_orders on orders.uuid = external_orders.uuid
       left join {{ source('int_service_supply', 'quicklinks_tracking') }}  qt on qt.order_uuid = orders.uuid
       left join fed_publicapi.quick_link ql on ql.quote_id = quotes.uuid
       where (ql.quote_id is not null or
           is_external='true' or qt.order_uuid is not null)
