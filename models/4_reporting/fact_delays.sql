-- Update: July 2022 (Diego)

{{ config(
    tags=["multirefresh"]
) }}

-- This table contains data about delays reported in the platform,
-- when an order is delayed or is expected to be delayed the MP
-- can proactively submit the delay(s) and assign a liability.
-- A delay can be submitted also by a user from Hubs.

select d.uuid                                                                                    as delay_uuid,
       d.created                                                                                 as delay_created_at,
       d.order_uuid,
       d.reason                                                                                  as delay_reason,
       d.description                                                                             as delay_description,
       d.new_shipping_date                                                                       as new_shipping_at,
       d.liability                                                                               as delay_liability,
       u.user_role_mapped                                                                        as delay_submitted_by,
       case when delay_liability <> 'supplier' and u.user_role_mapped = 'supplier' then true end as delay_requires_validation,
       true                                                                                      as delay_is_valid -- To be replaced with product feature enhancement (Diego, Aug 2022)
from {{ source('int_service_supply', 'order_delays') }} as d
    -- Joins to get role submitted the delay, prodct might add the column directly in the future
    left join {{ ref('prep_users') }} as u
on d.author_id = u.user_id
where d.deleted is null
    -- Data from the feature was not consistent before this date,
    -- contained several duplicates and liability column didn't existed.
  and d.created >= '2022-04-01'