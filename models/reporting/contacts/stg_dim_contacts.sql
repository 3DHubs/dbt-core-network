select dc.*,
       ad.advertising_gclid,
       ad.advertising_msclkid,
       ad.advertising_click_date,
       ad.advertising_click_device,
       ad.advertising_source,
       ad.advertising_account_id,
       ad.advertising_campaign_id,
       ad.advertising_adgroup_id, -- set adgroup_id to null for clicks that have no keyword id to make sure those clicks are properly joined in Looker
       ad.advertising_keyword_id,
       ad.advertising_campaign_group
from {{ ref('stg_contacts_owners') }} dc
            left outer join {{ ref('stg_contacts_advertising_data') }} ad on dc.hs_contact_id = ad.hs_contact_id