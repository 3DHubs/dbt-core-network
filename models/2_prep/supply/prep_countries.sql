
select issc.created,
       issc.updated,
       issc.deleted,
       issc.country_id,
       issc.name,
       issc.alpha2_code,
       issc.continent,
       issc.currency_code,
       issc.coordinates,
       issc.lat,
       issc.lon,
       --sub_region
       case 
            when lower(issc.alpha2_code) in ('be','lu','nl','dk','fi','no','se','ie','gb') then 'neur'
            when lower(issc.alpha2_code) in ('at','de','ch','pl') then 'ceur'
            when lower(issc.alpha2_code) in ('es','it','pt','fr') then 'seur'
            when lower(issc.alpha2_code) in ('ai','aw','bs','bb','cw','do','ag','bz','bm','bq','vg','ca','ky','cr','cu','dm','sv','gl','gp','mq','ms','an','pr','mf','pm','sx','tc','gd','gt','ht','hn','jm','mx','ni','pa','bl','kn','lc','vc','tt','vi','us') then 'na'
            else 'row'
            end sub_region,
       --region
       case 
            when sub_region = 'na' then 'na'
            else 'emea'
            end region,
       --market
       case when lower(issc.alpha2_code) in ('be','lu','nl') then 'benelux'
            when lower(issc.alpha2_code) in ('dk','fi','no','se') then 'nordics'
            when lower(issc.alpha2_code) in ('at','de','ch','pl') then 'dach'
            when lower(issc.alpha2_code) = 'fr' then 'france'
            when lower(issc.alpha2_code) in ('ie','gb') then 'uki'
            when lower(issc.alpha2_code) in ('ca','us','mx') then 'us/ca/mx'
            when lower(issc.alpha2_code) in ('es','it','pt') then 'iberia p /it'
            else 'row'
            end market,
            scmm.country_iso3,
            {{ varchar_to_boolean("is_in_eu") }},
            {{ varchar_to_boolean("has_payment_embargo") }},
            {{ varchar_to_boolean("is_in_efta") }},
            {{ varchar_to_boolean("is_in_european_union") }}

from {{ source('int_service_supply', 'countries') }} as issc
left join {{ source('int_analytics', 'supply_countries_markets_mapping')}} as scmm on lower(issc.alpha2_code) = scmm.country_iso2
left join {{ref('seed_countries_european_union')}} as  eu_union on issc.country_id = eu_union.country_id