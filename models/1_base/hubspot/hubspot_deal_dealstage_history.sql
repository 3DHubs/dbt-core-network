{{ 
    config(materialized='table'
    )
}}
with hubspot_dealstage_union as
         (
             select null as primary_key,
                    deal_id,
                    dealstage_mapped,
                    changed_at
             from {{source('ext_hubspot','hubspot_deals_dealstage_history_20201125')}}
             where changed_at < '2020-06-09'
             union
             select __sdc_primary_key                                             as primary_key,
                    objectid                                                      as deal_id,
                    hd.dealstage_mapped_value                                     as dealstage_mapped,
                    timestamp 'epoch' + (occurredat / 1000) * interval '1 second' as changed_at
             from {{source('ext_hubspot_webhooks','data')}} wd
                      left join {{ref('seed_hubspot_dealstages')}} hd
                                on hd.dealstage_internal_label = wd.propertyvalue
             where timestamp 'epoch' + (occurredat / 1000) * interval '1 second' >= '2020-06-09 00:00:00.000000'
             and propertyname='dealstage'
         )
select primary_key,
       deal_id,
       dealstage_mapped,
       lead(dealstage_mapped ignore nulls) over (partition by deal_id order by changed_at) as next_dealstage,
       changed_at,
       lead(changed_at ignore nulls) over (partition by deal_id order by changed_at)       as next_changed_at,
       datediff(minutes, changed_at,next_changed_at) as time_in_stage_minutes
from hubspot_dealstage_union
where dealstage_mapped is not null
order by deal_id, changed_at asc