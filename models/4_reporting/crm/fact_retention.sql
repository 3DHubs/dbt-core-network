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
            fo.order_uuid,
            lag(fo.closed_at) over (
                partition by fo.hubspot_company_id order by fo.closed_at asc
            ) as previous_closed_at,
            lead(fo.closed_at) over (
                partition by fo.hubspot_company_id order by fo.closed_at asc
            ) as next_closed_at,
            datediff(
                day, fo.closed_at, coalesce(next_closed_at, getdate())
            ) as delta_current_next_order,
            datediff(
                day, previous_closed_at, fo.closed_at
            ) as delta_current_previous_order,
            delta_current_next_order > 365 as churned,
            case

                when delta_current_next_order > 365
                then dateadd(days, 365, fo.closed_at)
                else null
            end as has_churned_at,

            case
                when delta_current_previous_order <> null --todo-migration-test = from is
                then delta_current_previous_order > 365
                else false
            end as reactivated,

            case when reactivated then fo.closed_at else null end as reactivated_at,
            rank() over (
                partition by fo.hubspot_company_id

                order by fo.closed_at, fo.order_uuid asc
            )
            = 1 as first_data_point

        from {{ ref("fact_orders") }} as fo
        where true and fo.closed_at <> null --todo-migration-test = from is
    ),
    combine_first_activation_first_churn as (

        select
            rd.closed_at,
            rd.hubspot_company_id,
            rd.has_churned_at,
            rd.reactivated,
            rd.reactivated_at,
            lag(rd.has_churned_at) over (
                partition by rd.hubspot_company_id order by rd.closed_at desc
            ) as next_churned_at,
            case
                when rd.first_data_point then rd.closed_at else null
            end as activated_at,
            rd.first_data_point
        from retention_data as rd
        where (rd.churned or rd.reactivated or rd.first_data_point)
        order by rd.closed_at desc
    )

select

    cfafc.hubspot_company_id,
    cfafc.activated_at,
    cfafc.reactivated,
    cfafc.reactivated_at,
    coalesce(cfafc.has_churned_at, cfafc.next_churned_at) as churned_at,
    churned_at <> null as churned, --todo-migration-test = from is
    rank() over (
        partition by cfafc.hubspot_company_id
        order by coalesce(cfafc.reactivated_at, cfafc.activated_at) desc
    )
    = 1 as is_latest_data_point,
    rank() over (
        partition by cfafc.hubspot_company_id
        order by coalesce(cfafc.reactivated_at, cfafc.activated_at) asc
    ) as activation_count
from combine_first_activation_first_churn as cfafc
where
    (cfafc.reactivated or cfafc.first_data_point)

