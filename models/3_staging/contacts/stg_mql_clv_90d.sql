with
    contacts as (
        select
            hubspot_contact_id,
            advertising_gclid,
            advertising_account_id,
            became_customer_at,
            became_mql_at,
            advertising_click_date,
            first_closed_order_technology,
            coalesce(
                case
                    when mql_technology in ('Casting', 'IM', 'Urethane Casting')
                    then 'CNC'
                    else mql_technology
                end,
                '3DP'
            ) as mql_technology,
            coalesce(region, 'row') as region,
            case
                when is_part_of_company = false or is_part_of_company is null
                then 'freemailer'
                when is_part_of_company = true and inside_mql_number = 1
                then 'first_company_customer'
                when is_part_of_company = true and inside_mql_number > 1
                then 'inside_company_customer'
                else 'first_company_customer'
            end as contact_type

        from {{ ref("dim_contacts") }}
    ),
    clv as (
        select
            contact_type,
            c.first_closed_order_technology,
            c.region,
            count(distinct c.hubspot_contact_id) as cohort_size,
            coalesce(
                sum((subtotal_sourced_amount_usd - po_first_sourced_cost_usd)), 0
            ) as precalculated_margin,
            precalculated_margin * 1.0 / cohort_size * 0.93 as clv_90d

        from {{ ref("fact_orders") }} fo
        inner join contacts c on c.hubspot_contact_id = fo.hubspot_contact_id
        where
            fo.sourced_at < date_add('days', 90, c.became_customer_at)
            and c.became_customer_at
            > date_add('years', -2, date_add('days', -90, getdate()))
            and c.became_customer_at < date_add('days', -90, getdate())
        group by 1, 2, 3
    ),  -- select * from clv;
    mql_pred as (
        select
            hubspot_contact_id,
            predicted_proba,
            row_number() over (
                partition by hubspot_contact_id order by model_executed_at asc
            ) as row
        from {{ source("data_lake", "mql_conversion_pred") }} pred
    )
select
    dc.hubspot_contact_id,
    dc.became_mql_at,
    dc.mql_technology,
    dc.region,
    dc.contact_type,
    advertising_gclid,
    advertising_account_id,
    advertising_click_date,
    clv.clv_90d,
    coalesce(predicted_proba*0.81, 0.0001) as is_customer_prediction,
    is_customer_prediction * clv_90d as cpa_price
from contacts dc
left join
    mql_pred pred on dc.hubspot_contact_id = pred.hubspot_contact_id and pred.row = 1
left join
    clv
    on clv.first_closed_order_technology = dc.mql_technology
    and clv.region = dc.region
    and clv.contact_type = dc.contact_type
where dc.became_mql_at >= '2021-01-01'
