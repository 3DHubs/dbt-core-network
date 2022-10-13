-- This model will be used to filter out test order data in the various objects e.g. fact_sales_orders.
select
       quotes.uuid,
       quotes.order_uuid,
       quotes.document_number,
       case when quotes.created < '2022-10-01' or lower(consumer_purchase_order_number) ~ 'test'  then true else false end is_test,
       decode(is_external, 'true', True, 'false', False) as is_integration,
       external_orders.consumer_order_id                                                      as integration_order_id, 
       external_orders.consumer_order_number                                                  as integration_order_number, 
       external_orders.consumer_purchase_order_number                                         as integration_purchase_order_number,  
       external_orders.consumer_order_created_at                                              as integration_order_created_at, 
       external_orders.consumer_ship_by                                                       as integration_order_ship_by_at


from {{ source('int_service_supply', 'cnc_order_quotes') }}  as quotes
       inner join {{ source('int_service_supply', 'cnc_orders') }} as orders on orders.quote_uuid = quotes.uuid
       left join {{ ref('addresses') }} as addresses_destination on quotes.shipping_address_id = addresses_destination.address_id
       left join {{ source('int_service_supply', 'external_orders') }} as external_orders on orders.uuid = external_orders.uuid
       where is_external='true'
