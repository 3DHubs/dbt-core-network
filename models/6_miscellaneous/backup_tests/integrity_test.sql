----------------------------------------------------------------
-- Overall Integrity Check
----------------------------------------------------------------

-- Use case:
-- This table contains a union of different data integrity tests 
-- Each test intends to ensure that the data does not change historically 
-- Furthermore, each test provides a descriptive error report if the integrity test fails 

-- Integrity format per column:
-- test_type                - provides a short description of what data the test is validating
-- identifier_type          - states the type of identifier used for the related record
-- identifier               - states the unique identifier of the record for which the integrity test failed
-- integrity_test_result    - describes the cause of the integrity failure
-- comparison_explanation   - describes what information is compared in the subsequent columns
-- comparison_backup        - provides the values present in the backup
-- comparison_production    - provides the values present in production

-- Last updated: July 6, 2022
-- Maintained by: Daniel Salazar Soplapuco


-- This code sets a local variable called date_constraint which is used to limit the range of the integrity test to a maximum date.
-- The maximum date is set on the last completed fiscal month, thus if the current day of the month is on or after the 10th
-- then the test will run until previous month otherwise it will run until 2 months ago.
{% if execute %}
{% set date_constraint = run_query("select to_char(add_months(date_trunc('month', getdate()), case when DATE_PART('day', getdate()) >= 10 then 0 else -1 end),'YYYYMMDD')")[0][0] %}
{% endif %}

--------------------------------------------
-- Full order history events integrity test
--------------------------------------------

-- Use case:
-- This test ensures that the historical order history events in production remain unchanged.

-- Method:
-- The test compares if the number of events in production per month remain equal to the events in the backups.
-- In the event that the number of events per month remain the same but the events themselves are adjusted
-- then this would arise errors in the cm1 integrity test.

with order_history_events_integrity_test as (
    with production_order_history_events as (
        select date_trunc('day', fohe.created) as created_at, count(*) as number_of_events
        from {{ source('data_lake', 'full_order_history_events') }} as fohe
        group by 1
    ),
        backups_order_history_events as (
            select date_trunc('day', bfohe.created) as created_at, count(*) as number_of_events
            from {{ source('dbt_backups', 'backup_full_order_history_events') }} as bfohe
            group by 1
        )

    select 'Order history events integrity check'                               as test_description,
        'Not applicable'                                                     as identifier_type,
        'Not applicable'                                                     as identifier,
        'Missing order events'                                               as integrity_test_result,
        'Comparing the number of events in day: ' ||
        cast(trunc(coalesce(b_ohe.created_at, p_ohe.created_at)) as varchar) as comparison_explanation,
        cast(b_ohe.number_of_events as varchar)                              as comparison_backup,
        cast(p_ohe.number_of_events as varchar)                              as comparison_production
    from production_order_history_events as p_ohe
            full join backups_order_history_events as b_ohe on p_ohe.created_at = b_ohe.created_at
    where b_ohe.number_of_events != p_ohe.number_of_events
    and b_ohe.created_at < to_date({{date_constraint}}, 'YYYYMMDD')
    and p_ohe.created_at < to_date({{date_constraint}}, 'YYYYMMDD')
),

----------------------
-- Cm1 integrity test
----------------------

-- Use case:
-- This test ensures that the historical cm1 data in production ensures remain unchanged.

-- Method:
-- The test compares the cm1 data present in the dbt_backups table against the cm1 data in production.
-- Through comparing records on its existence, recognized date and recognized amounts.
-- For it to trigger an error on recognized revenue amount the absolute difference has to be bigger than 100 Dollars

cm1_integrity_test as (

    select 'cm1 values integrity test'                 as test_description,
        'cm1 source document uuid:'                 as identifier_type,
        coalesce(bfcm.source_uuid, fcm.source_uuid) as identifier,
        case
            when (bfcm.source_uuid is null != fcm.source_uuid is null) then
                case
                    when bfcm.recognized_date is not null then 'Record does not exist in production'
                    when fcm.recognized_date is not null then 'Record does not exist in the backup' end
            when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                then 'Recognized date have changed'
            when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                then 'Recognized amounts have changed'
            end                                     as integrity_test_result,

        case
            when (bfcm.source_uuid is null != fcm.source_uuid is null)
                then 'Comparing Source UUID: '
            when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                then 'Comparing Recognized Date: '
            when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                then 'Comparing Amount USD: '
            end                                     as comparison_explanation,

        case
            when (bfcm.source_uuid is null != fcm.source_uuid is null)
                then bfcm.source_uuid
            when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                then cast(bfcm.recognized_date as varchar)
            when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                then cast(bfcm.amount_usd as varchar)
            end                                     as comparison_backup,

        case
            when (bfcm.source_uuid is null != fcm.source_uuid is null)
                then fcm.source_uuid
            when (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-01'))
                then cast(fcm.recognized_date as varchar)
            when (coalesce(bfcm.amount_usd, -0.1) != coalesce(fcm.amount_usd, -0.1))
                then cast(fcm.amount_usd as varchar)
            end                                     as comparison_production
    from {{ source('dbt_backups', 'backup_fact_contribution_margin') }} as bfcm
    full join {{ ref('fact_contribution_margin') }} as fcm  on bfcm.source_uuid = fcm.source_uuid
    where ((bfcm.source_uuid is null != fcm.source_uuid is null)
        or (coalesce(bfcm.recognized_date, '1000-01-01') != coalesce(fcm.recognized_date, '1000-01-02'))
        or abs((coalesce(bfcm.amount_usd, 0) - coalesce(fcm.amount_usd, 0)))  > 100
        )
    and fcm.recognized_date < to_date({{date_constraint}}, 'YYYYMMDD')
    and bfcm.recognized_date < to_date({{date_constraint}}, 'YYYYMMDD')
),

---------------------------------------------
-- Fact orders closing values integrity test
---------------------------------------------

-- Use case:
-- This test ensures that the historical fact orders closing data in production remain unchanged.

-- Method:
-- The test compares the fact orders closing data present in the dbt_backups table against the data in production.
-- Through comparing records on its existence, closing date and closed amounts.

fact_orders_integrity_test as (

select 'Fact Orders closing values integrity test' as test_description,
       'order_uuid:'                               as identifier_type,
       coalesce(bfo.order_uuid, fo.order_uuid)     as identifier,
       case
           when (bfo.order_uuid is null != fo.order_uuid is null) then
               case
                   when bfo.order_uuid is not null then 'Record does not exist in production'
                   when fo.order_uuid is not null then 'Record does not exist in the backup' end
           when (coalesce(bfo.closed_at, '1000-01-01') != coalesce(fo.closed_at, '1000-01-01'))
               then 'Closed date have changed'
           when (coalesce(bfo.subtotal_closed_amount_usd, -0.1) != coalesce(fo.subtotal_closed_amount_usd, -0.1))
               then 'Closed amounts have changed'
           end                                     as integrity_test_result,

       case
           when (bfo.order_uuid is null != fo.order_uuid is null)
               then 'Comparing Order UUID: '
           when (coalesce(bfo.closed_at, '1000-01-01') != coalesce(fo.closed_at, '1000-01-01'))
               then 'Comparing Closed Date: '
           when (coalesce(bfo.subtotal_closed_amount_usd, -0.1) != coalesce(fo.subtotal_closed_amount_usd, -0.1))
               then 'Comparing Closed Amount USD: '
           end                                     as comparison_explanation,

       case
           when (bfo.order_uuid is null != fo.order_uuid is null)
               then bfo.order_uuid
           when (coalesce(bfo.closed_at, '1000-01-01') != coalesce(fo.closed_at, '1000-01-01'))
               then cast(bfo.closed_at as varchar)
           when (coalesce(bfo.subtotal_closed_amount_usd, -0.1) != coalesce(fo.subtotal_closed_amount_usd, -0.1))
               then cast(bfo.subtotal_closed_amount_usd as varchar)
           end                                     as comparison_backup,

       case
           when (bfo.order_uuid is null != fo.order_uuid is null)
               then fo.order_uuid
           when (coalesce(bfo.closed_at, '1000-01-01') != coalesce(fo.closed_at, '1000-01-01'))
               then cast(fo.closed_at as varchar)
           when (coalesce(bfo.subtotal_closed_amount_usd, -0.1) != coalesce(fo.subtotal_closed_amount_usd, -0.1))
               then cast(fo.subtotal_closed_amount_usd as varchar)
           end                                     as comparison_production
from {{ source('dbt_backups', 'backup_fact_orders') }} as bfo
full join {{ ref('fact_orders') }} as fo on bfo.order_uuid = fo.order_uuid
where ((bfo.order_uuid is null != fo.order_uuid is null)
    or (coalesce(bfo.closed_at, '1000-01-01') != coalesce(fo.closed_at, '1000-01-01'))
    or (coalesce(bfo.subtotal_closed_amount_usd, 0) != coalesce(fo.subtotal_closed_amount_usd, 0)))
  and fo.closed_at < to_date({{date_constraint}}, 'YYYYMMDD')
  and bfo.closed_at < to_date({{date_constraint}}, 'YYYYMMDD')
  and fo.is_closed and bfo.is_closed
),

---------------------------------------------
-- Dim Companies Integrity Test
---------------------------------------------

-- Use case:
-- This test ensures that count of company records are consistent with the backup. 

-- Method:
-- This test aggregates count of created companies at a daily level
-- and compares the count between production and the backup schema.


dim_companies_integrity_test as (

    with production_companies as (
        select created_at::date as created_date, count(*) as number_of_companies
        from {{ ref('dim_companies') }} as pdc
        group by 1
    ),
        backup_companies as (
            select created_at::date as created_date, count(*) as number_of_companies
            from {{ source('dbt_backups', 'backup_dim_companies') }} as bdc
            group by 1
        )

    select 'Dim Companies Count Test'                                           as test_description,
        'Not applicable'                                                     as identifier_type,
        'Not applicable'                                                     as identifier,
        'Missing Companies'                                                  as integrity_test_result,
        'Comparing the number of companies in month: ' ||
        cast(trunc(coalesce(bdc.created_date, pdc.created_date)) as varchar) as comparison_explanation,
        cast(bdc.number_of_companies as varchar)                             as comparison_backup,
        cast(pdc.number_of_companies as varchar)                             as comparison_production
    from production_companies as pdc
            full join backup_companies as bdc on pdc.created_date = bdc.created_date
    where true
    -- Difference of 1% between backup and production
    and (abs(bdc.number_of_companies - pdc.number_of_companies)/bdc.number_of_companies) > 0.01
    and bdc.created_date < to_date({{date_constraint}}, 'YYYYMMDD')
    and pdc.created_date < to_date({{date_constraint}}, 'YYYYMMDD')
),

---------------------------------------------
-- Dim Contacts Integrity Test
---------------------------------------------

-- Use case:
-- This test ensures that count of contact records are consistent with the backup. 

-- Method:
-- This test aggregates count of created contact at a daily level
-- and compares the count between production and the backup schema.


dim_contacts_integrity_test as (

    with production_contacts as (
        select created_at::date as created_date, count(*) as number_of_contacts
        from {{ ref('dim_contacts') }} as pdc
        group by 1
    ),
        backup_contacts as (
            select created_at::date as created_date, count(*) as number_of_contacts
            from {{ source('dbt_backups', 'backup_dim_contacts') }} as bdc
            group by 1
        )

    select 'Dim Contacts Count Test'                                            as test_description,
        'Not applicable'                                                     as identifier_type,
        'Not applicable'                                                     as identifier,
        'Missing Contacts'                                                   as integrity_test_result,
        'Comparing the number of contacts in month: ' ||
        cast(trunc(coalesce(bdc.created_date, pdc.created_date)) as varchar) as comparison_explanation,
        cast(bdc.number_of_contacts as varchar)                              as comparison_backup,
        cast(pdc.number_of_contacts as varchar)                              as comparison_production
    from production_contacts as pdc
            full join backup_contacts as bdc on pdc.created_date = bdc.created_date
    where true
    -- Difference of 1% between backup and production
    and (abs(bdc.number_of_contacts - pdc.number_of_contacts)/bdc.number_of_contacts) > 0.01    
    and bdc.created_date < to_date({{date_constraint}}, 'YYYYMMDD')
    and pdc.created_date < to_date({{date_constraint}}, 'YYYYMMDD')
)

-- Union of all integrity tests
select * from order_history_events_integrity_test
union all
select * from cm1_integrity_test
union all 
select * from fact_orders_integrity_test
union all
select * from dim_companies_integrity_test
union all
select * from dim_contacts_integrity_test