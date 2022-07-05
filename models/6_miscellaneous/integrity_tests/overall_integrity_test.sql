with cm1_integrity_test as (
    select coalesce(bfcm.source_uuid, fcm.source_uuid)                       as source_uuid,
           coalesce(bfcm.source_document_number, fcm.source_document_number) as document_number,
           case
               when (bfcm.source_uuid is null != fcm.source_uuid is null) then
                   case
                       when bfcm.recognized_date is not null then 'Record does not exist in production'
                       when fcm.recognized_date is not null then 'Record does not exist in the backup' end
               when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                   then 'Recognized date have changed'
               when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                   then 'Recognized amounts have changed'
               end                                                           as integrity_test_result,

           case
               when (bfcm.source_uuid is null != fcm.source_uuid is null)
                   then 'Comparing Source UUID: '
               when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                   then 'Comparing Recognized Date: '
               when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                   then 'Comparing Amount USD: '
               end                                                           as comparison_explanation_1,

           case
               when (bfcm.source_uuid is null != fcm.source_uuid is null)
                   then bfcm.source_uuid
               when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                   then cast(bfcm.recognized_date as varchar)
               when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                   then cast(bfcm.amount_usd as varchar)
               end                                                           as comparison_backup_1,

           case
               when (bfcm.source_uuid is null != fcm.source_uuid is null)
                   then fcm.source_uuid
               when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                   then cast(fcm.recognized_date as varchar)
               when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                   then cast(fcm.amount_usd as varchar)
               end                                                           as comparison_production_1,

           'Comparing Recognition Date:'                                     as comparison_explanation_2,
           bfcm.recognized_date                                              as comparison_backup_2,
           fcm.recognized_date                                               as comparison_production_2,
           'Comparing Recognized Amount:'                                    as comparison_explanation_3,
           bfcm.amount_usd                                                   as comparison_backup_3,
           fcm.amount_usd                                                    as comparison_production_3
    from {{ ref('fact_contribution_margin') }} as fcm
         full join {{ source('dbt_backups', 'backup_fact_contribution_margin') }} as bfcm on bfcm.source_uuid = fcm.source_uuid
    where ((bfcm.source_uuid is null != fcm.source_uuid is null)
       or (coalesce (bfcm.recognized_date,'1000-01-01') != coalesce (fcm.recognized_date, '1000-01-02'))
       or (coalesce (bfcm.amount_usd, 0) != coalesce (fcm.amount_usd, 0)))
      and fcm.recognized_date
        < (select max (bfcm.recognized_date) from {{ source('dbt_backups', 'backup_fact_contribution_margin') }} as bfcm)
    order by coalesce (fcm.recognized_date, bfcm.recognized_date) desc
)
select *
from cm1_integrity_test
/* limit added automatically by dbt cloud */