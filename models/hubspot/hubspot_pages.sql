{{
    config(
        materialized='incremental'
    )
}}

with pages_prep as (
select date, 
       {{ dbt_utils.get_url_host(field='url') }} as host,
       url,
       case when len({{ dbt_utils.get_url_path(field='url') }})  < 2 then '/' else '/' + {{ dbt_utils.get_url_path(field='url') }} + '/' end as pages,
       coalesce(spg.page_group, spgs.page_group, 'Ungrouped')                                       page_group,
       case when len(split_part(pages, '/', 2)) = 2 then replace(split_part(pages, '/', 2),'js','en') else 'en' end as language,
       sum(entrances)                                                                            as entrances,
       sum(to_number(nullif(pageviews,''), '99999D9S'))::int                                     as pageviews,
       avg(round(to_number(nullif(time_per_pageview,''), '99999D9')))                            as time_per_pageview,
       avg(case when bounce_rate not like '%.%' then round(to_number(nullif(replace(bounce_rate,'%',''),''), '99'),2)
       else round(to_number(nullif(replace(bounce_rate,'%',''),''), '99D99'),2) end/100)         as bounce_rate


from {{ source('data_lake', 'hubspot_pages_staging') }} hp
         left join  {{ref('seo_page_groups')}} spg
                   on spg.page = lower(case when len({{ dbt_utils.get_url_path(field='url') }})  < 2 then '/' else '/' + {{ dbt_utils.get_url_path(field='url') }} + '/' end )
         left join  {{ref('seo_page_groups')}} spgs
                   on spgs.page = lower(case when urlsplit(url, 'path') = '/' then '/' else urlsplit(url, 'path') + '/' end)

where url <> ''
and
     ( entrances > 0 or to_number(nullif(pageviews,''), '99999D9S') > 0 or
       to_number(nullif(time_per_pageview,''), '99999D9') > 0 or
       replace(bounce_rate,'%','-')  like '%-%')

{% if is_incremental() %}

  and date > (select max(date) from {{ this }})

{% endif %}
group by 1,2,3,4,5)
select *,
       time_per_pageview * pageviews as time_per_pageview_prep,  
       bounce_rate * pageviews as bounce_rate_prep,
       {{ dbt_utils.get_url_parameter(field='url', url_parameter='abt') }}                    as test_name,
       {{ dbt_utils.get_url_parameter(field='url', url_parameter='abv') }}                    as test_variant
from pages_prep
where host in (
'www.3dhubs.com',
'www.hubs.com',
'help.3dhubs.com',
'help.hubs.com'
)