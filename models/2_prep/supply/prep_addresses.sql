-- Enrich addresses data with custom countries data (e.g. region, market)

select a.created,
       a.updated,
       a.deleted,
       a.address_id,
       a.address_line1,
       a.address_line2,
       a.administrative_area,
       a.lat,
       a.lon,
       a.locality,
       a.postal_code,
       a.country_id,
       a.country_name,
       a.country_alpha2_code,
       a.continent,
       a.company_name,
       a.first_name,
       a.last_name,
       a.email,
       a.timezone,
       c.sub_region,
       c.region,
       c.market
from {{ ref('addresses') }} as a
left join {{ ref('prep_countries') }} as c on a.country_id = c.country_id