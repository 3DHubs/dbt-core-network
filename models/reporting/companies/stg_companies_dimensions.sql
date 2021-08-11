select distinct hs_company_id,
       first_value(country_iso2)
       over ( -- selects the country from the oldest contact of the company that has a country available
       partition by hs_company_id order by (country_iso2 is null)::int, created_date asc rows between unbounded preceding and unbounded following)                               as country_iso2,
       first_value(market)
       over ( -- selects the country from the oldest contact of the company that has a country available
       partition by hs_company_id order by (country_iso2 is null)::int, created_date asc rows between unbounded preceding and unbounded following)                               as market,
       first_value(continent)
       over ( -- selects the country from the oldest contact of the company that has a country available
       partition by hs_company_id order by (country_iso2 is null)::int, created_date asc rows between unbounded preceding and unbounded following)                               as continent,
       first_value(region)
       over ( -- selects the country from the oldest contact of the company that has a country available
       partition by hs_company_id order by (country_iso2 is null)::int, created_date asc rows between unbounded preceding and unbounded following)                               as region,
       first_value(channel_grouped)
       over ( partition by hs_company_id order by least(hutk_analytics_first_visit_timestamp::timestamp, created_date) asc rows between unbounded preceding and unbounded following) as channel_grouped,
       first_value(first_quote_technology)
       over ( partition by hs_company_id order by became_opportunity_date asc rows between unbounded preceding and unbounded following)                                              as first_quote_technology,
       first_value(first_order_technology)
       over ( partition by hs_company_id order by became_opportunity_date asc rows between unbounded preceding and unbounded following)                                              as first_order_technology

from {{ ref('dim_contacts') }}