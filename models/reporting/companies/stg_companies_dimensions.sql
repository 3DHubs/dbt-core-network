select distinct hubspot_company_id,
                first_value(country_iso2)
                over ( -- selects the country from the oldest contact of the company that has a country available
                    partition by hubspot_company_id order by (country_iso2 is null)::int, created_date asc rows between unbounded preceding and unbounded following) as country_iso2,
                first_value(channel_grouped)
                over ( partition by hubspot_company_id
                    order by least(hutk_analytics_first_visit_timestamp::timestamp, created_date) asc rows between unbounded preceding and unbounded following)      as channel_grouped

from {{ ref('dim_contacts') }}
where hubspot_company_id is not null
  and not (channel_type = 'outbound' and lifecyclestage in ('lead', 'subscriber'))
