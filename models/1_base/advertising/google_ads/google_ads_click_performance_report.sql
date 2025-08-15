{{ config(
          pre_hook=["
            delete
            from analytics.ext_google_ads_console.click_performance_report
            where _sdc_sequence in (
                with to_remove as (
                    select _sdc_sequence,
                        row_number() over (partition by date, click_view_gclid order by _sdc_batched_at desc) as row_number
                    from INGESTION_SANDBOX_S3.ext_google_ads_console.click_performance_report)
            select _sdc_sequence
            from to_remove
            where row_number > 1) 
        "],
            ) }}

select distinct
       ad_group_name as adgroup,
       ad_group_id                                  as adgroup_id,
       campaign_name as campaign,
       campaign_id                                  as campaign_id,
       customer_id                                   as customer_id,
       date                                          as date,
       device,
       click_view_gclid                             as google_click_id,
       reverse(split_part(reverse(click_view_keyword),'~',1))::bigint   as keyword_id, --todo-migration-research reverse function
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