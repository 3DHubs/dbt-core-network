with attributable_contacts_numbered as (
    select row_number()
           over ( partition by hubspot_company_id
               order by least(hutk_analytics_first_visit_timestamp::timestamp, created_at) asc)                                                               as contact_number,
           first_value(country_iso2)
           over ( -- selects the country from the oldest contact of the company that has a country available
               partition by hubspot_company_id order by (country_iso2 is null)::int, created_at asc rows between unbounded preceding and unbounded following) as country_iso2,
           hubspot_contact_id,
           hubspot_company_id,
           channel_type,
           channel,
           channel_grouped,
           channel_drilldown_1,
           channel_drilldown_2,
           first_page_seen_grouped,
           advertising_gclid,
           advertising_msclkid,
           advertising_click_date,
           advertising_click_device,
           advertising_source,
           advertising_account_id,
           advertising_campaign_id,
           advertising_adgroup_id,
           advertising_keyword_id,
           advertising_campaign_group

    from {{ ref('dim_contacts') }}
    where hubspot_company_id is not null
      and not (channel_type = 'outbound' and lifecyclestage in ('lead', 'subscriber'))
)
select distinct hubspot_company_id,
                country_iso2,
                channel_type,
                channel,
                channel_grouped,
                channel_drilldown_1,
                channel_drilldown_2,
                first_page_seen_grouped,
                advertising_gclid,
                advertising_msclkid,
                advertising_click_date,
                advertising_click_device,
                advertising_source,
                advertising_account_id,
                advertising_campaign_id,
                advertising_adgroup_id,
                advertising_keyword_id,
                advertising_campaign_group
from attributable_contacts_numbered
where hubspot_company_id is not null
  and contact_number = 1

