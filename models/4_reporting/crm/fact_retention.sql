-- --------------------------------------------------------------
-- Marketing/Sales - Fact Retention
-- --------------------------------------------------------------
-- Table use case summary:
-- This table intends to show every important data point on retention.
-- Such as first order placed, reactivation and churning.
-- Reactivation is defined as a company closing a new order after 365 days of not
-- having placed an order.
-- Churn is defined as a company not having closed an order in the last 365 days.
-- Last updated: April 2, 2023.
-- Maintained by: Daniel Salazar
with
    retention_data as (

        select
            fo.hubspot_company_id,
            fo.closed_at,
            lag(fo.closed_at) over (
                partition by fo.hubspot_company_id order by fo.closed_at asc
            ) as previous_closed_at_date,
            lead(fo.closed_at) over (
                partition by fo.hubspot_company_id order by fo.closed_at asc
            ) as next_closed_at_date,
            datediff(
                day, fo.closed_at, coalesce(next_closed_at_date, getdate())
            ) as delta_current_next_order,
            datediff(
                day, previous_closed_at_date, fo.closed_at
            ) as delta_current_previous_order,
            delta_current_next_order > 365 as churned,
            case
                when churned then dateadd(days, 365, fo.closed_at) else null
            end as churned_at,
            case
                when delta_current_previous_order is not null
                then delta_current_previous_order > 365
                else false
            end as reactivated,

            case when reactivated then fo.closed_at else null end as reactivated_at,
            rank() over (
                partition by fo.hubspot_company_id
                order by fo.closed_at, fo.order_uuid desc
            )
            = 1 as is_latest_data_point,

            rank() over (
                partition by fo.hubspot_company_id
                order by fo.closed_at, fo.order_uuid asc
            )
            = 1 as is_first_data_point

        from {{ ref("fact_orders") }} as fo
        where true and fo.closed_at is not null
    )

select
    case when is_first_data_point then rd.closed_at else null end as activated_at,
    rd.hubspot_company_id,
    rd.churned,
    rd.churned_at,
    rd.reactivated,
    rd.reactivated_at,
    rd.is_latest_data_point,
    rd.is_first_data_point
from retention_data as rd
where (churned or reactivated or is_latest_data_point or is_first_data_point)
