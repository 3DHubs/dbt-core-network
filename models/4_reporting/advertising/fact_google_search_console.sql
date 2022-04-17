{{
    config(
        materialized='incremental'
    )
}}

with stg_gsc_data as (
    select date,
           case when keys ~ 'https.{3}www.3dhubs.com' then '3dhubs' else 'hubs' end tld,
           translate(keys, '[]', '')                                 as keys_clean,
           split_part(regexp_replace(trim(translate(split_part(keys_clean, ',', 1), '''', '')), 'https://www.{1,3}hubs.com', ''), '#', 1) -- Catering for 3dhubs.com and hubs.com
                                                                     as url,
           trim(translate(split_part(keys_clean, ',', 2), '''', '')) as country_iso3,
           trim(translate(split_part(keys_clean, ',', 3), '''', '')) as keywords,
           position::decimal(15, 1)                                  as position,
           impressions,
           clicks
    from {{ source('data_lake', 'google_search_console') }}
    where true
      and dimension1 = 'page'
      and dimension2 = 'country'
      and dimension3 = 'query'

{% if is_incremental() %}

  and date > (select max(date) from {{ this }})

{% endif %}
),
stg_join_w_country as (
    select gsc.date,
           gsc.tld,
           gsc.keys_clean,
           case when len({{ dbt_utils.get_url_path(field='gsc.url') }})  < 2 then '/' else replace(('/' + {{ dbt_utils.get_url_path(field='gsc.url') }} + '/'),'//','/') end url,
           case when len(split_part(url, '/', 2)) = 2 then split_part(url, '/', 2) else 'en' end as language,
           gsc.country_iso3                      as country_code,
           gsc.keywords,
           coalesce(spg.page_group, 'Ungrouped') as page_group,
           gsc.position,
           gsc.impressions,
           gsc.clicks,
           scmm.market,
           scmm.name                             as country,
           gsc.url || gsc.keywords               as target_id
    from stg_gsc_data as gsc
             left join {{ref('seed_countries_mapping')}} as scmm on scmm.country_iso3 = gsc.country_iso3
             left join {{ref('seed_seo_page_groups')}} as spg on spg.page = lower(gsc.url)
),
stg_join_w_seo_targets as (
    select gsc_country.*,
           case when gsct.target_id is not null then true else false end is_seo_target
    from stg_join_w_country as gsc_country
             left join {{source('data_lake', 'static_seo_targets')}} as gsct on gsct.target_id = gsc_country.target_id
)

select *
from stg_join_w_seo_targets
