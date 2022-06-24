
----------------------------------------------------------------
-- Logistics - Fact Aftership Messages
----------------------------------------------------------------

-- Table use case summary:
-- The table displays all the status messages received for each of our shipments (example: Label created, Picked up by the carrier, Delivered)
-- This data is used to understand the journey a shipment goes through and proactively identify any issues which might arise during shipment

-- Last updated: May 31, 2022
-- Maintained by: Daniel Salazar

-- Sources:
-- Aftership Message

{{ config(
    tags=["multirefresh"]
) }}

with fact_aftership_messages_bv as (
    select 'BV'                                               as entity,
           exa.msg__tracking_number                           as carrier_tracking_number, -- EXA EXt Aftership
           exa.msg__courier_tracking_link                     as carrier_tracking_link,
           timestamptz(exa.msg__expected_delivery)::timestamp as tracking_latest_expected_delivery,
           timestamptz(exam.checkpoint_time)::timestamp       as tracking_message_received_at,
           exam.city                                          as tracking_shipment_city,  -- EXAM EXt Aftership Messages
           exam.country_name                                  as tracking_shipment_country_name,
           exam.location                                      as tracking_shipment_location,
           exam.subtag_message                                as tracking_status,
           exam.message                                       as tracking_message,

           -- The where "first_occurrence_of_message" is used to ensure that you only capture the first occurrence of a message, as the same message can occur several times sequentially.
           -- Therefore, to limit the number of duplicate messages we only filter on the first occurrence of a message in a sequence.

           row_number()
           OVER (PARTITION BY exa.msg__tracking_number, cast(exam.checkpoint_time as date), exam.location, exam.message ORDER BY
               exam.checkpoint_time asc) = 1                  as first_occurrence_of_message
    from {{ source('ext_aftership_bv', 'data') }} as exa
             left join {{ source('ext_aftership_bv', 'data__msg__checkpoints') }} as exam
    on exa.__sdc_primary_key = exam._sdc_source_key___sdc_primary_key
    order by exam.checkpoint_time asc
),
     fact_aftership_messages_llc as (
         select 'LLC'                                              as entity, -- EXA EXt Aftership
                exa.msg__tracking_number                           as carrier_tracking_number,
                exa.msg__courier_tracking_link                     as carrier_tracking_link,
                timestamptz(exa.msg__expected_delivery)::timestamp as tracking_latest_expected_delivery,
                timestamptz(exam.checkpoint_time)::timestamp       as tracking_message_received_at,
                exam.city                                          as tracking_shipment_city, -- EXAM EXt Aftership Messages
                exam.country_name                                  as tracking_shipment_country_name,
                exam.location                                      as tracking_shipment_location,
                exam.subtag_message                                as tracking_status,
                exam.message                                       as tracking_message,
                -- This indicates if it is the first occurence of the message or the subsequent dupplicates
                row_number()
                OVER (PARTITION BY exa.msg__tracking_number, cast(exam.checkpoint_time as date), exam.location, exam.message ORDER BY
                    exam.checkpoint_time asc) = 1                  as first_occurrence_of_message
         from {{ source('ext_aftership_llc', 'data') }} as exa
             left join {{ source('ext_aftership_llc', 'data__msg__checkpoints') }} as exam
         on exa.__sdc_primary_key = exam._sdc_source_key___sdc_primary_key
         order by exam.checkpoint_time asc
     ),
     fact_aftership_messages_union as (
         select *
         from fact_aftership_messages_bv as fam
-- This where filter removes dupplicate messages
         where fam.first_occurrence_of_message
         union
         select *
         from fact_aftership_messages_llc as fam
         where fam.first_occurrence_of_message
     )

select famu.*, -- FAMU Fact Aftership Messages Unionised
       -- Field: has_logistics_message_alert identifies if the message contains words which are highly associated with delays which are resolvable by logistics.
       regexp_count(lower(' '||famu.tracking_message||' '),
                    '( restriction | attention | missing | company | name | e | number | ave | contacted | investigation' ||
                    '| message | pre | funds | street | about | product | receiving | return | either | begun | locate |'||
                    ' invoice | improper | authorization | oversized | overweight | require | commercial | goods | commercial |'||
                    ' identification | express | invoice | claim | accident | situation | either | classify | commodity |'||
                    ' description | insufficient | breakdown | composition | itemized | original | get | opened | allowed |'||
                    ' maximum | reaching | remains | nees | company | ne | detailed | ein | etc | examples | gst | include |'||
                    ' registration | rfc | ssn | vat | lost | redelivery | reminder | highway | state | logo | f | llendorffstr |'||
                    ' mu | end | fwy | importation | indicating | reason | statement | stemmons | use | written | vista | cave |'||
                    ' colossal | leon | ne | ponce | fcringer | fcrnberg | nu | thu | centre | nw | form | specialized | statement )') >
       0                                                                                               as has_logistics_message_alert,

       -- Field: has_tracking_status_alert states if aftership indicates that the message has a high likelyhood to a delay.
       regexp_count(lower(famu.tracking_status), '(delay|external|exception|payment|held|damaged|lost)') >
       0                                                                                               as has_tracking_status_alert,

       row_number()
       OVER (PARTITION BY famu.carrier_tracking_number ORDER BY famu.tracking_message_received_at asc) AS message_number,
       count(*) over (PARTITION BY famu.carrier_tracking_number) =
       message_number                                                                                  as is_last_message,
       date_diff('hours', famu.tracking_message_received_at,
                 lead(famu.tracking_message_received_at, 1) OVER (PARTITION BY
                     famu.carrier_tracking_number ORDER BY famu.tracking_message_received_at asc))     as number_of_hours_in_status
from fact_aftership_messages_union as famu