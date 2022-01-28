-- JG 2022-01-028, FIX to populate orders without contact id, but with company id with the contact that was first created for the company.
select distinct order_hubspot_deal_id,
                fo.hubspot_company_id,
                submitted_at,
                first_value(dc.hubspot_contact_id)
                over (partition by fo.hubspot_company_id
                    order by dc.created_at asc
                    rows between unbounded preceding and unbounded following) as hubspot_contact_id
from dbt_prod_reporting.fact_orders fo
         left join dbt_prod_reporting.dim_contacts dc on dc.hubspot_company_id = fo.hubspot_company_id
where fo.hubspot_contact_id is null
  and fo.hubspot_company_id is not null
  and submitted_at is not null
  and is_legacy = false