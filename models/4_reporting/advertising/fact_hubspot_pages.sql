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
       coalesce(spg.page_group, 'Ungrouped')                                                     as page_group,
       case when len(split_part(pages, '/', 2)) = 2 then replace(split_part(pages, '/', 2),'js','en') else 'en' end as language,
       sum(entrances)                                                                            as entrances,
       sum(pageviews)                                                                            as pageviews,
       avg(time_per_pageview)                                                                    as time_per_pageview,
       avg(bounce_rate)                                                                          as bounce_rate


from {{ref('hubspot_pages')}} hp
         left join  {{ref('seed_seo_page_groups')}} spg
                   on spg.page = lower(case when len({{ dbt_utils.get_url_path(field='url') }})  < 2 then '/' else '/' + {{ dbt_utils.get_url_path(field='url') }} + '/' end )

where url <> ''
and
     ( entrances > 0 or pageviews > 0 or
       time_per_pageview > 0 or
       bounce_rate > 0)

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