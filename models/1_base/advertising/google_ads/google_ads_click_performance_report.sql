-- todo-migration: return prehook. 

select distinct
       ad_group_name as adgroup,
       ad_group_id                                  as adgroup_id,
       campaign_name as campaign,
       campaign_id                                  as campaign_id,
       customer_id                                   as customer_id,
       date                                          as date,
       device,
       click_view_gclid                             as google_click_id,
       reverse(split_part(reverse(click_view_keyword),'~',1))::bigint   as keyword_id,
       click_view_keyword_info__text                as keyword
from {{ source('ext_google_ads_console', 'click_performance_report') }}
union
select distinct
       adgroup,
       adgroupid                                    as adgroup_id,
       campaign,
       campaignid                                   as campaign_id,
       customerid                                   as customer_id,
       day                                          as date,
       device,
       googleclickid                                as google_click_id,
       keywordid                                    as keyword_id,
       keywordplacement                             as keyword
from {{ source('ext_adwords', 'click_performance_report') }}
where googleclickid not in (select click_view_gclid from  {{ source('ext_google_ads_console', 'click_performance_report') }})
union
select null,
       ad_group_id::bigint                 as adgroup_id,
       null                                as campaign,
       campaign_id::bigint                 as campaign_id,
       adwords_customer_id::bigint         as customer_id,
       date_start                          as date,
       device,
       gcl_id                              as google_click_id,
       original_keyword_id::bigint         as keyword_id,
       criteria_parameters                 as keyword
from {{ source('adwords', 'click_performance_reports') }} -- Data originating from Segment
where gcl_id not in (select googleclickid from  {{ source('ext_adwords', 'click_performance_report') }})