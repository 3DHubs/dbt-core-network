with stg as (
    select hubspot_contact_id,
           order_closed_at,
           count(order_uuid) over (partition by hubspot_contact_id)                     as total_number_of_quotes,
           count(case when order_is_closed then order_uuid end)
           over (partition by hubspot_contact_id)                                       as total_number_of_closed_orders,
           max(order_closed_at) over (partition by hubspot_contact_id)                      as recent_closed_order_date,
           lag(order_closed_at)
           over (partition by hubspot_contact_id order by order_closed_at)                  as previous_closed_order_date
    from {{ ref('stg_fact_orders') }}
)

select hubspot_contact_id,
        min(total_number_of_quotes)                                        as total_number_of_quotes,
        min(total_number_of_closed_orders)                                    total_number_of_closed_orders,
        min(recent_closed_order_date)                                      as recent_closed_order_date,
        round(avg(extract(day from order_closed_at - previous_closed_order_date)),
            1)                                                           as average_days_between_closed_orders,
        median(extract(day from order_closed_at - previous_closed_order_date)) as median_days_between_closed_orders

from stg

group by hubspot_contact_id