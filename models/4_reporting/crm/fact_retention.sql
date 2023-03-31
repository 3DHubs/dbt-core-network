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
            end as churn_date,
            case
                when delta_current_previous_order is not null
                then delta_current_previous_order > 365
                else false
            end as re_activated,

            case when re_activated then fo.closed_at else null end as re_activated_date,
            rank() over (partition by fo.hubspot_company_id order by fo.closed_at, fo.order_uuid desc)
            = 1 as is_latest_data_point

        from {{ ref("fact_orders") }} as fo
        where true and fo.closed_at is not null
    )

select
    rd.hubspot_company_id,
    rd.churned,
    rd.churn_date,
    rd.re_activated,
    rd.re_activated_date,
    rd.is_latest_data_point
from retention_data as rd
where (churned or re_activated or is_latest_data_point)
