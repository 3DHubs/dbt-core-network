-- This table is not materialized in the database

{{ 
    config(
        materialized='ephemeral'
    )
}}

with  quarterly_closed_amount_directors as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      case when fo.destination_market != 'us/ca' then 'Neil Gilbody' else 'Anthony Giampapa' END as director,
      -- c.reports_to_lead                                                                    as lead_report,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)         as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2021-01-01'
      group by 1,2
      ),
      quarterly_target_directors as (
      select date_trunc('quarter', cr.date)                                                           as target_date,
      cr.hubspot_id,
      cr.region,
      cr.name as employee,
      sum(monthly_target) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }} cr
      where role='director'
      group by 1,2,3,4),
      quarterly_directors as (
      select
      date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      target_amount_usd, subtotal_closed_amount_usd,
      'Quarterly Director'::text as commission_plan,
      case when subtotal_closed_amount_usd > target_threshold then true else false end                            as quarterly_bonus_to_be_paid,
      subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0) as percent_of_target,
      case when quarterly_bonus_to_be_paid = true and percent_of_target < 1 then (percent_of_target-0.75)/0.25
      when quarterly_bonus_to_be_paid = true and percent_of_target >=1 then percent_of_target  end as bonus_ratio,
      coalesce((20000*bonus_ratio),1)  as commission_usd

      from quarterly_closed_amount_directors qa
      inner join quarterly_target_directors qt on qt.employee = qa.director and qt.target_date = qa.commission_date),
      quarterly_closed_amount_leads as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      c.reports_to_lead                                                                     as lead_report,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)         as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      inner join {{ ref('commission_rules') }} c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fo.hubspot_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2021-01-01'
      group by 1,2
      ),
      quarterly_target_leads as (
      select date_trunc('quarter', cr.date)                                                           as target_date,
      cr.reports_to_lead,
      dr.name as employee,
      dr.role,
      sum(case when cr.role = 'strategic lead' then monthly_lead_target else monthly_target end) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }} cr
      inner join (select hubspot_id, name, date, role from {{ ref('commission_rules') }} where role in ('strategic lead','inside lead')) dr on dr.hubspot_id = cr.reports_to_lead and dr.date = cr.date
      where cr.role = ('strategic lead') or dr.role='inside lead'
      group by 1,2,3,4),
      quarterly_leads as (
      select
       date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      target_amount_usd, subtotal_closed_amount_usd,
      'Quarterly Leads'::text as commission_plan,
      case when subtotal_closed_amount_usd > target_threshold then true else false end                            as quarterly_bonus_to_be_paid,
      subtotal_closed_amount_usd *1.0 / target_amount_usd as percent_of_target,
      case when quarterly_bonus_to_be_paid = true and percent_of_target < 1 then (percent_of_target-0.75)/0.25
      when quarterly_bonus_to_be_paid = true and percent_of_target >=1 then percent_of_target  end as bonus_ratio,
      coalesce((case when role = 'inside lead' and employee = 'Dan Scahill'  then
          case when quarterly_bonus_to_be_paid then subtotal_closed_amount_usd * 0.2 / 100 else 1 end else
      case when role = 'inside lead' then  3000
      when role = 'strategic lead' then 6000  *bonus_ratio end end),1)  as commission_usd

      from quarterly_closed_amount_leads qa
      inner join quarterly_target_leads qt on qt.reports_to_lead = qa.lead_report and qt.target_date = qa.commission_date),

      ------------------------------------ QUARTERLY COMMISSIONS ----------------------------------------

      quarterly_closed_amount as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      case when role = 'inside' then owner.hubspot_owner_id else fo.hubspot_owner_id end     as hubspot_owner_id,
      case when role = 'inside' then own.name  else fo.hubspot_owner_name end   as employee,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)                  as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      left join dbt_prod_snapshots.snap_dim_companies_hubspot_owner owner on fo.hubspot_company_id = owner.hubspot_company_id and closed_at >= owner.dbt_valid_from and closed_at < coalesce(owner.dbt_valid_to, getdate())
      inner join {{ ref('commission_rules') }} c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fo.hubspot_owner_id
      left join dbt_prod_core.hubspot_owners own on own.owner_id = owner.hubspot_owner_id
      where true
      and is_strategic = true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2021-01-01'
      group by 1,2,3
      ),
      quarterly_target as (
      select date_trunc('quarter', date)                                                           as target_date,
      hubspot_id,
      quarterly_fee,
      above_quarterly_target_fee,
      sum(monthly_target) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }}
      where role in ('strategic','strategic lead')
      group by 1,2,3,4),
      quarterly_strategic_prep as (
      select
      commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      case when subtotal_closed_amount_usd > target_threshold then true else false end                            as quarterly_bonus_to_be_paid,
      subtotal_closed_amount_usd - target_amount_usd as quarterly_overperformance,
      coalesce(case when quarterly_bonus_to_be_paid = true then round(subtotal_closed_amount_usd * qt.quarterly_fee,2) end,1) as strategic_closed_sales_commission,
      coalesce(case when quarterly_bonus_to_be_paid = true and quarterly_overperformance > 0 then round(quarterly_overperformance * qt.above_quarterly_target_fee,2)  end,1) as over_performance
      from quarterly_closed_amount qa
      inner join quarterly_target qt on qt.hubspot_id = qa.hubspot_owner_id and qt.target_date = qa.commission_date),
      quarterly_strategic as (
      select  date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      'Quarterly Strategic'::text as commission_plan,
      sum(strategic_closed_sales_commission)  as commission_usd
      from quarterly_strategic_prep
      where true
      and commission_date is not null
      and strategic_closed_sales_commission is not null
      group by commission_date, employee
      ),
      quarterly_overperformance as (
      select  date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      'Quarterly Strategic'::text as commission_plan,
      sum(over_performance)  as commission_usd
      from quarterly_strategic_prep
      where true
      and commission_date is not null
      and over_performance is not null
      group by commission_date, employee
      )

      ------------------------------------ SPECIAL COMMISSIONS ----------------------------------------

      ------------------------------------ FINAL QUARTERLY TABLE --------------------------------------------

  
      select commission_date,
      order_hubspot_deal_id,
      employee,
      commission_plan,
      commission_usd
      from quarterly_directors
      union all
      select commission_date,
      order_hubspot_deal_id,
      employee,
      commission_plan,
      commission_usd
      from quarterly_leads
      union all
      select *
      from quarterly_strategic
      union all
      select *
      from quarterly_overperformance
      