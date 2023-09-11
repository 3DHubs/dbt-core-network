-- This table is not materialized in the database

{{ 
    config(
        materialized='ephemeral'
    )
}}

with  quarterly_closed_amount_directors as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      case when fo.destination_region != 'na' then 'Neil Gilbody' else 'Anthony Giampapa' end as director,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)       as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2022-01-01'
      group by 1,2
      ),
      quarterly_target_directors as (
      select date_trunc('quarter', cr.date)                                                           as target_date,
      cr.name as employee,
      sum(monthly_target) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }} cr
      where role='director'
      group by 1,2),
      quarterly_directors as (
      select
      date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      target_amount_usd, 
      subtotal_closed_amount_usd,
      'Quarterly Director'::text as commission_plan,
      case when subtotal_closed_amount_usd > target_threshold then true else false end as quarterly_bonus_to_be_paid,
      ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5 as percent_of_target,
      case when quarterly_bonus_to_be_paid = true then coalesce((20000*bonus),1)  end as commission_usd

      from quarterly_closed_amount_directors qa
      inner join quarterly_target_directors qt on qt.employee = qa.director and qt.target_date = qa.commission_date
      left join {{ ref('seed_sales_targets_staffel') }} on on_target =  (case when ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) > 1.5 then 1.5
          else round(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) end)
      ),
      ------------------------------ INTEGRATION LEAD ------------------------------------
      quarterly_closed_amount_integration_lead as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      case when fo.destination_region = 'na' then 'Austin Daugherty'  end as integration_lead,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)       as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2022-01-01'
      and fo.destination_region = 'na'
      group by 1,2
      ),
      quarterly_target_integration_lead as (
      select date_trunc('quarter', cr.date)                                                           as target_date,
      cr.name as employee,
      sum(monthly_lead_target) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }} cr
      where role='integration lead'
      group by 1,2),
      quarterly_integration_lead as (
      select
      date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      target_amount_usd, 
      subtotal_closed_amount_usd,
      'Integration Lead'::text as commission_plan,
      case when subtotal_closed_amount_usd > target_threshold then true else false end as quarterly_bonus_to_be_paid,
      ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5 as percent_of_target,
      case when quarterly_bonus_to_be_paid = true then coalesce((7000*bonus),1)  end as commission_usd

      from quarterly_closed_amount_integration_lead qa
      inner join quarterly_target_integration_lead qt on qt.employee = qa.integration_lead and qt.target_date = qa.commission_date
      left join {{ ref('seed_sales_targets_staffel') }} on on_target =  (case when ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) > 1.5 then 1.5
          else ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) end) 
      ),
      ------------------------------ TECHNICAL SALES MANAGER ------------------------------------
      quarterly_closed_amount_technical_manager as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      case when fo.destination_region != 'na' then 'Philippe Tarjan'  end as technical_manager,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)       as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2022-01-01'
      and fo.destination_region != 'na'
      group by 1,2
      ),
      quarterly_target_technical_manager as (
      select date_trunc('quarter', cr.date)                                                           as target_date,
      cr.name as employee,
      sum(monthly_lead_target) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }} cr
      where role='technical manager'
      group by 1,2),
      quarterly_technical_manager as (
      select
      date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      target_amount_usd, 
      subtotal_closed_amount_usd,
      'Technical Sales Manager'::text as commission_plan,
      case when subtotal_closed_amount_usd > target_threshold then true else false end as quarterly_bonus_to_be_paid,
      ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5 as percent_of_target,
      case when quarterly_bonus_to_be_paid = true then coalesce((15000*bonus),1)  end as commission_usd

      from quarterly_closed_amount_technical_manager qa
      inner join quarterly_target_technical_manager qt on qt.employee = qa.technical_manager and qt.target_date = qa.commission_date
      left join {{ ref('seed_sales_targets_staffel') }} on on_target =  (case when ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) > 1.5 then 1.5
          else ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) end) 
      ),
      ------------------------------ STRATEGIC LEAD ------------------------------------
      quarterly_closed_amount_leads as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      c.reports_to_lead                                                                     as lead_report,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)         as subtotal_closed_amount_usd
      from dbt_prod_reporting.fact_orders fo
      inner join {{ ref('commission_rules') }} c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fo.hubspot_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2022-01-01'
      group by 1,2
      ),
      quarterly_target_leads as (
      select date_trunc('quarter', cr.date)                                                           as target_date,
      cr.reports_to_lead,
      cr.name as employee,
      cr.role,
      cr.region,
      sum(case when cr.role in ('strategic lead','inside lead') then monthly_lead_target else monthly_target end) as target_amount_usd,
      round(target_amount_usd*0.75) as target_threshold
      from {{ ref('commission_rules') }} cr
      where cr.role in ('strategic lead','inside lead','support')
      group by 1,2,3,4,5),
      quarterly_leads as (
      select
       date_add('month',2,commission_date) as commission_date,
      null::bigint as order_hubspot_deal_id,
      employee,
      target_amount_usd, 
      subtotal_closed_amount_usd,
      'Quarterly Leads / Support'::text as commission_plan,
      case when subtotal_closed_amount_usd > target_threshold then true else false end                            as quarterly_bonus_to_be_paid,
       ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5 as percent_of_target,
      coalesce(case when quarterly_bonus_to_be_paid = true 
      and qt.role = 'inside lead' then 4000*bonus  
      when quarterly_bonus_to_be_paid = true 
      and qt.role = 'strategic lead' and qt.region='US' then 7500*bonus
      when quarterly_bonus_to_be_paid = true 
      and qt.role = 'strategic lead' and qt.region='EU' then 7500*bonus
      when quarterly_bonus_to_be_paid = true 
      and qt.role = 'support' and qt.region='EU' then 1500*bonus
      when quarterly_bonus_to_be_paid = true 
      and qt.role = 'support' and qt.region='US' then 3000*bonus  end,1)  as commission_usd

      from quarterly_closed_amount_leads qa
      inner join quarterly_target_leads qt on qt.reports_to_lead = qa.lead_report and qt.target_date = qa.commission_date
      left join {{ ref('seed_sales_targets_staffel') }} on on_target =  (case when ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) > 1.5 then 1.5
          else ROUND(ROUND((subtotal_closed_amount_usd *1.0 / nullif(target_amount_usd,0))/5,2) * 5,2) end) 
      ),

      ------------------------------------ QUARTERLY COMMISSIONS ----------------------------------------

      quarterly_closed_amount as (
      select date_trunc('quarter', closed_at)                                                      as commission_date,
      case when role = 'inside' then owner.hubspot_owner_id else fo.hubspot_owner_id end     as hubspot_owner_id,
      case when role = 'inside' then own.name  else fo.hubspot_owner_name end   as employee,
      round(sum( case when is_strategic then subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0) else 0 end) , 2)                  as subtotal_closed_amount_usd,
      round(sum(subtotal_closed_amount_usd - coalesce(shipping_amount_usd, 0)) , 2)                                                          as subtotal_threshold_amount_usd -- used for 75% threshold calculation
      from dbt_prod_reporting.fact_orders fo
      left join dbt_prod_snapshots.snap_dim_companies_hubspot_owner owner on fo.hubspot_company_id = owner.hubspot_company_id and closed_at >= owner.dbt_valid_from and closed_at < coalesce(owner.dbt_valid_to, getdate())
      inner join {{ ref('commission_rules') }} c on c.date = date_trunc('month', closed_at) and c.hubspot_id = fo.hubspot_owner_id
      left join dbt_prod_core.hubspot_owners own on own.owner_id = owner.hubspot_owner_id
      where true
      and hubspot_dealstage_mapped not in ('Closed - Canceled')
      and closed_at >='2023-01-01'
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
      case when subtotal_threshold_amount_usd > target_threshold then true else false end                            as quarterly_bonus_to_be_paid,
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
      from quarterly_integration_lead
      union all
      select commission_date,
      order_hubspot_deal_id,
      employee,
      commission_plan,
      commission_usd
      from quarterly_technical_manager
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
      