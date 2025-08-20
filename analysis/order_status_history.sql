-- JG 202508 legacy model , could be reviewed again in the future or deprecate if not touched in 6 months
with status as (SELECT order_uuid,
                       is_cross_docking,
                       status,
                       time_line::date as start_status_date,
                       coalesce(lag(start_status_date, 1)
                                over (partition by order_uuid order by start_status_date desc),
                           case when status not in ('completed_at', 'cancelled_at') then getdate()::date end) as end_status_date,
                       date_diff('day', start_status_date, end_status_date) as  status_period_in_days
                FROM (SELECT order_uuid,
                             is_cross_docking,
                             created_at,
                             submitted_at,
                             closed_at,
                             sourced_at,
                             order_shipped_at,
                             delivered_to_cross_dock_at,
                             shipped_from_cross_dock_at,
                             delivered_at,
                             dispute_created_at,
                             completed_at,
                             cancelled_at
                      FROM dbt_prod_reporting.fact_orders
                      where closed_at >= '2021-01-01') UNPIVOT (
                      time_line FOR status IN (created_at, submitted_at, closed_at, sourced_at,order_shipped_at, delivered_to_cross_dock_at, shipped_from_cross_dock_at, delivered_at, dispute_created_at, completed_at,cancelled_at)
                    ))
select s.order_uuid,
       status as date_point,
       start_status_date,
       end_status_date,
       status_period_in_days,
       case
           when s.status = 'created_at' then 'created'
           when s.status = 'submitted_at' then 'submitted'
           when s.status = 'closed_at' then 'accepted'
           when s.status = 'sourced_at' then 'in_production'
           when s.status = 'order_shipped_at' and is_cross_docking = false then 'shipped to customer'
           when s.status = 'order_shipped_at' and is_cross_docking = true then 'shipped to cross_dock'
           when s.status = 'delivered_to_cross_dock_at' then 'parts_inspected'
           when s.status = 'shipped_from_cross_dock_at' then 'shipped to customer'
           when s.status = 'delivered_at' then 'delivered to customer'
           when s.status = 'dispute_created_at' then 'disputed' end as status
from status s


--todo-migration-adhoc: hardcoded reference to Redshift schemas