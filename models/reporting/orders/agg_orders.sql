----------------------------------------------------------------
-- DEAL AGGREGATES
----------------------------------------------------------------

-- This table is built from the fact_orders table 
-- and later appended into the [type table name ] table.

select order_uuid,

    -- Customer Fields
    min(order_quote_submitted_at) over (partition by hubspot_contact_id)                        as became_opportunity_date_contact,
    min(order_closed_at) over (partition by hubspot_contact_id)                                 as became_customer_date_contact,


    -- Deal Fields
    case
        when is_closed_won is true
            then rank() over (partition by hubspot_contact_id order by order_closed_at asc) end as closed_deal_number_contact,

    round(extract(minutes from
                  (order_closed_at - lag(order_closed_at) over (partition by hubspot_contact_id order by
                      order_closed_at
                      asc))) / 1440,
          1)                                                                                as closed_deal_days_between_previous_deal_contact,
    round(extract(minutes from
                  (order_closed_at - lag(delivered_at) over (partition by hubspot_contact_id order by
                      order_closed_at
                      asc))) / 1440,
          1)                                                                                as closed_deal_days_between_previous_deal_from_delivery_contact,
    datediff('month', became_customer_date_contact, order_closed_at) = 0
                                                                                            as is_new_customer_contact


    -- Legacy Fields (Defined based on Client Legacy Concept)

--     became_opportunity_date
--     became_customer_date
--     closed_deal_number
--     closed_deal_days_between_previous_deal,
--     closed_deal_days_between_previous_deal_from_delivery
--     is_new_customer

from {{ ref('fact_orders') }} as fo
