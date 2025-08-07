select
    __sdc_primary_key,
    _sdc_batched_at,
    _sdc_received_at,
    _sdc_report_datetime,
    _sdc_sequence,
    _sdc_table_version,
    accountid,
    accountname,
    adgroupid,
    adgroupname,
    campaignid,
    campaignname,
    clicks,
    currencycode,
    impressions,
    keyword,
    keywordid,
    spend,
    timeperiod,
    'network_amer' as source
from {{ source('ext_bing', 'keyword_performance_report') }}
union all
-- todo-migration-missing: the table below is not available yet, add when available
{#
-- select
--     __sdc_primary_key,
--     _sdc_batched_at,
--     _sdc_received_at,
--     _sdc_report_datetime,
--     _sdc_sequence,
--     _sdc_table_version,
--     accountid,
--     accountname,
--     adgroupid,
--     adgroupname,
--     campaignid,
--     campaignname,
--     clicks,
--     currencycode,
--     impressions,
--     keyword,
--     keywordid,
--     spend,
--     timeperiod,
--     'factory' as source
-- from {{ source('_ext_bing_factory', 'keyword_performance_report') }}
-- union all
#}
select
    __sdc_primary_key,
    _sdc_batched_at,
    _sdc_received_at,
    _sdc_report_datetime,
    _sdc_sequence,
    _sdc_table_version,
    accountid,
    accountname,
    adgroupid,
    adgroupname,
    campaignid,
    campaignname,
    clicks,
    currencycode,
    impressions,
    keyword,
    keywordid,
    spend,
    timeperiod,
    'network_emea' as source
from {{ source('ext_bing_emea', 'keyword_performance_report') }}
