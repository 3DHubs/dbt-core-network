{{
    config(
        pre_hook = "analyze {{ ref('order_delays') }}",
        post_hook = "analyse {{ this }}"
    )
}}

/*
 * This model aggregates data on `created`, `uuid`,
 * and `description` to guarantee uniquness on these
 * attributes as there are some duplicate delay
 * entries. Note that `uuid` is a random value in case
 * there are duplicate issues but that should not
 * affect the data.
 */
select min(created) as  submitted_at,
       max(uuid)    as  delay_uuid,
       order_uuid,
       max(description) delay_description,
       new_shipping_date,
       reason       as  delay_category
from {{ ref('order_delays') }}
where deleted is null
group by 3, 5, 6