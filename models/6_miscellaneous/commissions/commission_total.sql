with all_commissions as (
      select *
      from {{ ref('commission_monthly') }} 
      union all
      select *
      from {{ ref('commission_quarterly') }} 
      )

      select commission_date,
      order_hubspot_deal_id,
      employee,
      commission_plan::text as commission_plan,
      commission_usd,
      own.owner_id
      from all_commissions
      left join dbt_prod_core.hubspot_owners own on own.name = all_commissions.employee and own.is_current is true
      where true
      and employee is not null
      and commission_date is not null
      and commission_usd <> 0
      order by commission_date desc, employee