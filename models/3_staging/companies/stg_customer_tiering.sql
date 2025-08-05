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
                      row_number() over (partition by hubspot_company_id order by snapshot_at asc) as row_ --todo-migration-test: idk best name for it
               from {{ source('int_analytics', 'customer_tiering') }} )
select * from tiers where row_ =1