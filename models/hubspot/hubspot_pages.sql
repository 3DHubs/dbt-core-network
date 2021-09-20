{{
    config(
        materialized='table'
    )
}}

select *,time_per_pageview * pageviews as time_per_pageview_prep,  bounce_rate * pageviews as bounce_rate_prep,
case when len({{ dbt_utils.get_url_path(field='url') }})  < 2 then '/' else '/' + {{ dbt_utils.get_url_path(field='url') }} + '/' end   as page_path,
{{ dbt_utils.get_url_parameter(field='pages', url_parameter='abt') }}                    as test_name,
{{ dbt_utils.get_url_parameter(field='pages', url_parameter='abv') }}                    as test_variant
from {{ source('data_lake', 'hubspot_pages_new') }}
