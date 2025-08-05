-- This model will be used to filter out test order data in the various objects e.g.
-- fact_sales_orders.
select
    quote.uuid,
    quote.order_uuid,
    quote.document_number,
    orders.is_external as is_papi_integration,
    case
        when is_external
        then 'papi'
        when ql.quote_id is not null
        then 'quicklink'
        when qt.is_quick_link
        then 'quicklink'
        else 'shallowlink'
    end as integration_platform_type,
    case when lower(consumer_purchase_order_number) = 'Med AZ Batt Test' then false -- exception for customer order that used test in PO
        when
            quote.created < '2022-10-01'
            or regexp_like(lower(consumer_purchase_order_number), 'test') --todo-migration-test
            or regexp_like(ql.request_id, 'test') --todo-migration-test
            or regexp_like(ql.email, 'mailinator') --todo-migration-test
            or ql.is_protolabs_email
            or ql.is_hubs_email
        then true
        when
            quote.created < '2023-04-17'
            and integration_platform_type = 'quicklink'
            and quote.shipping_country = 'United States'
        then true
        else false
    end is_test,
    external_orders.consumer_order_id as integration_order_id,
    ql.request_id as integration_quote_id,
    coalesce(
        external_orders.consumer_order_number, qt.pl_quote_number
    ) as integration_order_number,
    external_orders.consumer_purchase_order_number as integration_purchase_order_number,
    coalesce(
        external_orders.consumer_order_created_at, ql.created_at
    ) as integration_order_created_at,
    external_orders.consumer_ship_by as integration_order_ship_by_at,
    coalesce(qt.pl_user_id, ql.user_id) as integration_user_id,
    qt.utm_content as integration_utm_content,
    count(quote.order_uuid) over (partition by integration_platform_type, integration_order_number ) as number_of_orders_per_integration_order,
    case when number_of_orders_per_integration_order > 1 and integration_platform_type = 'papi' then true else false end as is_multi_line_papi_integration

from {{ ref("documents") }} as quote --todo-migration-test  doesn't work in dbt but works in Snowflake itself
inner join
    {{ ref("orders") }} as orders
    on orders.quote_uuid = quote.uuid
left join
    {{ ref('sources_network', 'gold_external_orders') }} as external_orders
    on orders.uuid = external_orders.uuid
left join
    {{ ref('sources_network', 'gold_quicklinks_tracking') }} as qt
    on qt.order_uuid = orders.uuid
left join
    {{ ref('sources_network', 'gold_quick_link') }} as ql
    on ql.quote_id = quote.uuid
    and ql.created_at < '2023-04-01'  -- switched to quicklinks_tracking after April
where (ql.quote_id is not null or orders.is_external or qt.order_uuid is not null) 
