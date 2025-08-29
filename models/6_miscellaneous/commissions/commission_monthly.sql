{{ 
    config(
        materialized='ephemeral'
    )
}}

with deal_monthly as (
            select date_trunc('month', closed_at)                                                         as commission_date,
                   order_hubspot_deal_id,
                   hubspot_owner_name                                                                     as employee,
                   'Deal Monthly Base'::text                                                                    as commission_plan,
                    round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd

      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.hubspot_owner_id
      where true
      and case
      when hubspot_dealstage_mapped <> 'Closed - Canceled' then true
      when (hubspot_dealstage_mapped = 'Closed - Canceled' and
      date_trunc('month', cancelled_at) = date_trunc('month', closed_at)) then false
      when (hubspot_dealstage_mapped = 'Closed - Canceled' and
      date_trunc('month', cancelled_at) <> date_trunc('month', closed_at)) then true
      end
      group by commission_date, employee, order_hubspot_deal_id
      ),
      bdr_deal_monthly as (
            select date_trunc('month', closed_at)                                                         as commission_date,
                   order_hubspot_deal_id,
                   bdr_owner_name                                                                     as employee,
                   'BDR Monthly Base'::text                                                                    as commission_plan,
                    round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* (c.monthly_fee * case when closed_at > '2024-04-01' then 2 else 1 end)) , 2) as commission_usd

      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.bdr_owner_id
      where true
      and bdr_owner_id = hubspot_owner_id
      and case
      when hubspot_dealstage_mapped <> 'Closed - Canceled' then true
      when (hubspot_dealstage_mapped = 'Closed - Canceled' and
      date_trunc('month', cancelled_at) = date_trunc('month', closed_at)) then false
      when (hubspot_dealstage_mapped = 'Closed - Canceled' and
      date_trunc('month', cancelled_at) <> date_trunc('month', closed_at)) then true
      end
      and role = 'outside'
      group by commission_date, employee, order_hubspot_deal_id
      ),

      ---------------- DEALS EXCLUDED FROM CURRENT MONTH (BIG DEALS, SVP & UNDER-QUOTES) ---------------

      big_deals_excluded as (
      select date_trunc('month', closed_at)                                               as commission_date,
      order_hubspot_deal_id,
      hubspot_owner_name                                                           as employee,
      'Big Deals Excluded'::text                                                         as commission_plan,
      - round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.hubspot_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and subtotal_closed_amount_usd - shipping_amount_usd > 50000
      -- Only deals that have not yet been sourced should be excluded
      and ((date_trunc('month', sourced_at) > commission_date) or (sourced_at = null)) --todo-migration-test
      group by commission_date, employee,order_hubspot_deal_id
      ),
      significant_amount_gap_deals_excluded as (
      select date_trunc('month', closed_at)                                               as commission_date,
      order_hubspot_deal_id,
      hubspot_owner_name                                                           as employee,
      'Significant Amount Gap Deals Excluded'::text                                                as commission_plan,
      - round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.hubspot_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and has_significant_amount_gap
      group by commission_date, employee,order_hubspot_deal_id
      ),
      ---------------  Same as above for BDR --------------

      bdr_big_deals_excluded as (
      select date_trunc('month', closed_at)                                               as commission_date,
      order_hubspot_deal_id,
      bdr_owner_name                                                           as employee,
      'Big Deals Excluded'::text                                                         as commission_plan,
      - round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.bdr_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and bdr_owner_id = hubspot_owner_id
      and subtotal_closed_amount_usd - shipping_amount_usd > 50000
      -- Only deals that have not yet been sourced should be excluded
      and ((date_trunc('month', sourced_at) > commission_date) or (sourced_at = null)) --todo-migration-test
      and role = 'outside'
      group by commission_date, employee,order_hubspot_deal_id
      ),
      bdr_significant_amount_gap_deals_excluded as (
      select date_trunc('month', closed_at)                                               as commission_date,
      order_hubspot_deal_id,
      bdr_owner_name                                                           as employee,
      'Significant Amount Gap Deals Excluded'::text                                                as commission_plan,
      - round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.bdr_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and has_significant_amount_gap
      and bdr_owner_id = hubspot_owner_id
      and role = 'outside'
      group by commission_date, employee,order_hubspot_deal_id
      ),

      ---------------- COMMISSION AT SOURCING, DELIVERY & CANCELLATION DEDUCTIONS ---------------

      big_deals_commission as (
      select date_trunc('month', sourced_at)                                              as commission_date,
      order_hubspot_deal_id,
      hubspot_owner_name                                                           as employee,
      'Big Deals Commission'::text                                                       as commission_plan,
      round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.hubspot_owner_id
      where true
      and closed_at <> null --todo-migration-test
      and subtotal_closed_amount_usd - shipping_amount_usd > 50000
      and closed_at < commission_date
      group by commission_date, employee, order_hubspot_deal_id
      ),
      significant_amount_gap_deals_commission as (
      select date_trunc('month', delivered_at)        as commission_date,
      order_hubspot_deal_id,
      hubspot_owner_name                       as employee,
      'Significant Amount Gap Deals Commission'::text as commission_plan,
      round(sum(hubspot_amount_usd *  c.monthly_fee), 2)  as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.hubspot_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled', 'Closed - Lost')
      and closed_at <> null --todo-migration-test
      and has_significant_amount_gap
      group by commission_date, employee, order_hubspot_deal_id
      ),

      cancellation_deductions as (
      with monthly_cancellations as (
      select date_trunc('month', cancelled_at) as commission_date,
      order_hubspot_deal_id,
      date_trunc('month', closed_at),
      hubspot_owner_id,
      hubspot_owner_name                as employee,
      case
      when date_trunc('month', closed_at)
      between add_months(commission_date, -3) and add_months(commission_date, -1)
      then true end             as cancelled_deal_closed_three_months_prior,
      subtotal_closed_amount_usd,
      shipping_amount_usd
      from dbt_prod_reporting.fact_orders
      where true
      and cancelled_deal_closed_three_months_prior = true --todo-migration-test replaced is with =
      and cancelled_at <> null --todo-migration-test replaced is with =
      )
      select commission_date,
      order_hubspot_deal_id,
      employee,
      'Cancellations Deduction'::text                                                    as commission_plan,
      - round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from monthly_cancellations
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', commission_date) and c.hubspot_id = monthly_cancellations.hubspot_owner_id
      group by commission_date, employee, order_hubspot_deal_id
      ),

      ---------------- SAME AS ABOVE FOR BDR  ---------------

      bdr_big_deals_commission as (
      select date_trunc('month', sourced_at)                                              as commission_date,
      order_hubspot_deal_id,
      bdr_owner_name                                                           as employee,
      'Big Deals Commission'::text                                                       as commission_plan,
      round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.bdr_owner_id
      where true
      and closed_at <> null --todo-migration-test
      and bdr_owner_id = hubspot_owner_id
      and subtotal_closed_amount_usd - shipping_amount_usd > 50000
      and closed_at < commission_date
      and role = 'outside'
      group by commission_date, employee, order_hubspot_deal_id
      ),
      bdr_significant_amount_gap_deals_commission as (
      select date_trunc('month', delivered_at)        as commission_date,
      order_hubspot_deal_id,
      bdr_owner_name                       as employee,
      'Significant Amount Gap Deals Commission'::text as commission_plan,
      round(sum(hubspot_amount_usd *  c.monthly_fee), 2)  as commission_usd
      from dbt_prod_reporting.fact_orders
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fact_orders.bdr_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled', 'Closed - Lost')
      and bdr_owner_id = hubspot_owner_id
      and closed_at <> null --todo-migration-test
      and has_significant_amount_gap
      and role = 'outside'
      group by commission_date, employee, order_hubspot_deal_id
      ),

      bdr_cancellation_deductions as (
      with monthly_cancellations as (
      select date_trunc('month', cancelled_at) as commission_date,
      order_hubspot_deal_id,
      date_trunc('month', closed_at),
      bdr_owner_id,
      bdr_owner_name                as employee,
      case
      when date_trunc('month', closed_at)
      between add_months(commission_date, -3) and add_months(commission_date, -1)
      then true end             as cancelled_deal_closed_three_months_prior,
      subtotal_closed_amount_usd,
      shipping_amount_usd
      from dbt_prod_reporting.fact_orders
      where true
      and cancelled_deal_closed_three_months_prior = true --todo-migration-test replaced is with =
      and cancelled_at <> null --todo-migration-test replaced is with =
      and bdr_owner_id = hubspot_owner_id
      )
      select commission_date,
      order_hubspot_deal_id,
      employee,
      'Cancellations Deduction'::text                                                    as commission_plan,
      - round(sum((subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0))* c.monthly_fee) , 2) as commission_usd
      from monthly_cancellations
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', commission_date) and c.hubspot_id = monthly_cancellations.bdr_owner_id
      where role = 'outside'
      group by commission_date, employee, order_hubspot_deal_id
      ),
      ----------------------------------- RAMPING COMPENSATION _--------------------------------------
      ramping as (
      select date                            commission_date,
      null::bigint                 as order_hubspot_deal_id,
      cr.name                      as employee,
      'Ramping Compensation'::text as commission_plan,
      sum(compensation_value)      as commission_usd
      from {{ ref('commission_rules') }} cr
      group by 1, 2, 3, 4
      ),

      ----------------------------------- BDR HANDOVER --------------------------------------
      bdr_handover as (
      select date_trunc('month', outbound_handover_date)        as commission_date,
      null::bigint as order_hubspot_deal_id,
      hubspot_handover_owner_name                       as employee,
      'BDR Handover fee'::text as commission_plan,
       sum(
            case
                when outbound_handover_date < '2024-04-01' then 350
                when true_outbound then 400
                else 200
            end
        ) as commission_usd
      from dbt_prod_reporting.dim_companies
      inner join {{ ref('commission_rules') }}  c on c.date = date_trunc('month', outbound_handover_date) and c.name = dim_companies.hubspot_handover_owner_name
      where true
      and role = 'outside' and hubspot_owner_name <> null --todo-migration-test
      group by commission_date, employee, order_hubspot_deal_id
      ),

      ----------------------------------- TOTAL MONTHLY COMMISSION --------------------------------------
      deal_monthly_union as (
      select *
      from deal_monthly
      union all
      select *
      from bdr_deal_monthly
      union all
      select *
      from significant_amount_gap_deals_excluded
      union all
      select *
      from big_deals_excluded
      union all
      select *
      from big_deals_commission
      union all
      select *
      from significant_amount_gap_deals_commission
      union all
      select *
      from cancellation_deductions
      union all
      select *
      from bdr_significant_amount_gap_deals_excluded
      union all
      select *
      from bdr_big_deals_excluded
      union all
      select *
      from bdr_big_deals_commission
      union all
      select *
      from bdr_significant_amount_gap_deals_commission
      union all
      select *
      from bdr_cancellation_deductions
      union all
      select *
      from bdr_handover
      union all
      select *
      from ramping
      )
      select commission_date,
      order_hubspot_deal_id,
      employee,
      'Deal Monthly Total'::text as commission_plan,
      sum(commission_usd)  as commission_usd
      from deal_monthly_union
      where true
      and commission_date <> null --todo-migration-test
      and commission_usd <> null --todo-migration-test
      group by commission_date, employee, order_hubspot_deal_id
      union all
      select *
      from deal_monthly
      union all
      select *
      from bdr_deal_monthly
      union all
      select *
      from significant_amount_gap_deals_excluded
      union all
      select *
      from big_deals_excluded
      union all
      select *
      from big_deals_commission
      union all
      select *
      from significant_amount_gap_deals_commission
      union all
      select *
      from cancellation_deductions
      union all
      select *
      from bdr_significant_amount_gap_deals_excluded
      union all
      select *
      from bdr_big_deals_excluded
      union all
      select *
      from bdr_big_deals_commission
      union all
      select *
      from bdr_significant_amount_gap_deals_commission
      union all
      select *
      from bdr_cancellation_deductions
      union all
      select *
      from bdr_handover
      union all
      select *
      from ramping