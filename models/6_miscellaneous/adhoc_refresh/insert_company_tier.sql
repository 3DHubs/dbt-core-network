{{ config(bind=False,
          pre_hook=["
insert into dbt_prod_core.stg_customer_tiering
with tiers as (select hubspot_company_id,
                      snapshot_at,
                      case
                          when tiering_probability > percentile_cont(0.80)
                                                     within group (order by tiering_probability asc)
                                                     over (PARTITION BY snapshot_at) then 'high'
                          when tiering_probability > percentile_cont(0.60)
                                                     within group (order by tiering_probability asc)
                                                     over (PARTITION BY snapshot_at) then 'medium'
                          when tiering_probability <= percentile_cont(0.60)
                                                      within group (order by tiering_probability asc)
                                                      over (PARTITION BY snapshot_at) then 'low' end as potential_tier,
                      tiering_probability,
                      row_number() over (partition by hubspot_company_id order by snapshot_at asc) as row
               from data_lake.customer_tiering)
select * from tiers where row =1 and hubspot_company_id not in (select hubspot_company_id from dbt_prod_core.stg_customer_tiering)
limit 10
        "],
            ) }}
select 1