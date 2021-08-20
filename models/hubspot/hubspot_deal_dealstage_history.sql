with hubspot_dealstage_union as
         (
             select deal_id,
                    dealstage_mapped,
                    changed_at
             from {{source('landing','hubspot_deals_dealstage_history_20201125')}}
             where changed_at < '2020-06-09'
             union
             select objectid                                                      as deal_id,
                    hd.dealstage_mapped_value                                     as dealstage_mapped,
                    timestamp 'epoch' + (occurredat / 1000) * interval '1 second' as changed_at
             from {{source('ext_hubspot_webhooks','data')}} wd
                      left join {{ref('hubspot_dealstages')}} hd
                                on hd.dealstage_internal_label = wd.propertyvalue
             where timestamp 'epoch' + (occurredat / 1000) * interval '1 second' >= '2020-06-09 00:00:00.000000'
         )
select deal_id,
       dealstage_mapped,
       lag(dealstage_mapped ignore nulls) over (partition by deal_id order by changed_at) as previous_dealstage,
       changed_at,
       lag(changed_at ignore nulls) over (partition by deal_id order by changed_at)       as previous_changed_at
from hubspot_dealstage_union