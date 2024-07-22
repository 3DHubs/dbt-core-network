-- This model will be used to filter out test order data in the various objects e.g.
-- fact_sales_orders.
select
    quotes.uuid,
    quotes.order_uuid,
    quotes.document_number,
    decode(is_external, 'true', true, 'false', false) as is_papi_integration,
    case
        when is_external = 'true'
        then 'papi'
        when ql.quote_id is not null
        then 'quicklink'
        when qt.utm_campaign = 'plQuicklink'
        then 'quicklink'
        else 'shallowlink'
    end as integration_platform_type,
    case
        when
            quotes.created < '2022-10-01'
            or lower(consumer_purchase_order_number) ~ 'test'
            or ql.request_id ~ 'test'
            or ql.email ~ 'mailinator'
            or ql.email ~ 'protolabs'
            or ql.email ~ '@(3d)?hubs.com'
        then true
        when
            quotes.created < '2023-04-17'
            and integration_platform_type = 'quicklink'
            and adr.country_id = 237
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
    replace(qt.utm_content, 'content=', '') as integration_utm_content,
    count(quotes.order_uuid) over (partition by integration_platform_type, integration_order_number ) as number_of_orders_per_integration_order,
    case when number_of_orders_per_integration_order > 1 and integration_platform_type = 'papi' then true else false end as is_multi_line_papi_integration

from {{ source("int_service_supply", "cnc_order_quotes") }} as quotes
inner join
    {{ source("int_service_supply", "cnc_orders") }} as orders
    on orders.quote_uuid = quotes.uuid
left join
    {{ source("int_service_supply", "external_orders") }} as external_orders
    on orders.uuid = external_orders.uuid
left join
    {{ source("int_service_supply", "quicklinks_tracking") }} qt
    on qt.order_uuid = orders.uuid
left join
    fed_publicapi.quick_link ql
    on ql.quote_id = quotes.uuid
    and created_at < '2023-04-01'  -- switched to quicklinks_tracking after April
left join
    {{ ref("addresses") }} adr
    on adr.address_id = quotes.shipping_address_id
where (ql.quote_id is not null or is_external = 'true' or qt.order_uuid is not null)
