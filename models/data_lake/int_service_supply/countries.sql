{% set boolean_fields = [
    "is_in_eu",
    "has_payment_embargo",
    "is_in_efta"
    ]
%}

select created,
       updated,
       deleted,
       country_id,
       name,
       alpha2_code,
       continent,
       currency_code,
       coordinates,
       lat,
       lon,
       --region
       case when alpha2_code in ('al','ad','by','dk','ee','fi','ax','at','be','ba','bg','es-cn','hr','cy','cz','fo','fr','de','gi','gr','hu','ie','je','lv','lt','mk','mt','md','mc','no','pt','se','ch','ua','gg','is','im','it','li','lu','me','nl','pl','ro','ru','sm','rs','sk','si','es','sj','gb','va') then 'europe'
            when alpha2_code in ('ai','aw','bs','bb','cw','do','ag','bz','bm','bq','vg','ca','ky','cr','cu','dm','sv','gl','gp','mq','ms','an','pr','mf','pm','sx','tc','gd','gt','ht','hn','jm','mx','ni','pa','bl','kn','lc','vc','tt','vi','us') then 'north-america'
            else 'row'
            end region,
       --market
       case when alpha2_code in ('dk','fi','be','no','se','lu','nl') then 'benelux and nordics'
            when alpha2_code in ('at','de','ch') then 'dach'
            when alpha2_code = 'fr' then 'france'
            when alpha2_code in ('ie','gb') then 'uki'
            when alpha2_code in ('ca','us') then 'us/ca'
            else 'other'
            end market,
       {% for boolean_field in boolean_fields %}
           {{ varchar_to_boolean(boolean_field) }}
           {% if not loop.last %},{% endif %}
       {% endfor %}
from int_service_supply.countries