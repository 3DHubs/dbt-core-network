{% set boolean_fields = [
    "is_in_eu",
    "has_payment_embargo",
    "is_in_efta"
    ]
%}

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
       --region
       case when lower(issc.alpha2_code) in ('al','ad','by','dk','ee','fi','ax','at','be','ba','bg','es-cn','hr','cy','cz','fo','fr','de','gi','gr','hu','ie','je','lv','lt','mk','mt','md','mc','no','pt','se','ch','ua','gg','is','im','it','li','lu','me','nl','pl','ro','ru','sm','rs','sk','si','es','sj','gb','va') then 'europe'
            when lower(issc.alpha2_code) in ('ai','aw','bs','bb','cw','do','ag','bz','bm','bq','vg','ca','ky','cr','cu','dm','sv','gl','gp','mq','ms','an','pr','mf','pm','sx','tc','gd','gt','ht','hn','jm','mx','ni','pa','bl','kn','lc','vc','tt','vi','us') then 'north-america'
            else 'row'
            end region,
       --market
       case when lower(issc.alpha2_code) in ('dk','fi','be','no','se','lu','nl') then 'benelux and nordics'
            when lower(issc.alpha2_code) in ('at','de','ch','pl','cz','hu','sk') then 'dach'
            when lower(issc.alpha2_code) = 'fr' then 'france'
            when lower(issc.alpha2_code) in ('ie','gb') then 'uki'
            when lower(issc.alpha2_code) in ('ca','us') then 'us/ca'
            else 'other'
            end market,
            scmm.country_iso3,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}
from {{ source('int_service_supply', 'countries') }} as issc
left join {{ source('data_lake', 'supply_countries_markets_mapping')}} as scmm on lower(issc.alpha2_code) = scmm.country_iso2



