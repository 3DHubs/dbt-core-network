/*
 * This model aggregates data on `created`, `uuid`,
 * and `description` to guarantee uniqueness on these
 * attributes as there are some duplicate delay
 * entries. Note that `uuid` is a random value in case
 * there are duplicate issues but that should not
 * affect the data. 
 */


-- Update: January 2022 (Diego)
-- This data is currently being analysed at the order level but in the
-- future with the new setup of shipments/packages and batch shipments
-- we might want to reconsider checking this at the package level. 

select min(created) as  submitted_at,
       max(uuid)    as  delay_uuid,
       max(description) delay_description,
       order_uuid,
       new_shipping_date,
       reason       as  delay_category
from {{ source('int_service_supply', 'order_delays') }}
where deleted is null
group by 4, 5, 6