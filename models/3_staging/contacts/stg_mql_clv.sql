with
    clv as (
        with
            cohort_contacts as (
                with
                    contacts as (
                        select *
                        from {{ ref('dim_contacts') }}
                        where channel_grouped <> 'outbound'
                    )

                select
                    datediff('month', became_customer_at, getdate()) - 1 as cohort,
                    first_closed_order_technology,
                    region,
                    count(1) contacts,
                    sum(contacts) over (
                        partition by first_closed_order_technology, region
                        order by cohort desc
                        rows between 5 preceding and current row
                    ) as running_sum
                from contacts
                where
                    date_trunc('month', became_customer_at)
                    >= date_trunc('month', date_add('month', -30, getdate()))
                    and date_trunc('month', became_customer_at)
                    < date_trunc('month', date_add('month', -0, getdate()))
                group by 1, 2, 3
            ),
            cohort_sales as (
                select
                    datediff(
                        'month', fo.became_customer_at_contact, fo.sourced_at
                    ) cohort,
                    dcom.first_closed_order_technology,
                    dcom.region,
                    count(distinct(fo.hubspot_contact_id)) active_contacts,
                    coalesce(
                        sum((subtotal_sourced_amount_usd - po_first_sourced_cost_usd)),
                        0
                    ) as precalculated_margin
                from {{ ref("fact_orders") }} as fo
                left join
                    {{ ref("dim_contacts") }}  as dc
                    on fo.hubspot_contact_id = dc.hubspot_contact_id
                left join
                    {{ ref("dim_contacts") }}  as dcom
                    on dc.hubspot_contact_id = dcom.hubspot_contact_id
                where
                    dcom.channel_grouped <> 'outbound'
                    and (
                        (date_trunc('month', dcom.became_customer_at)) >= dateadd(
                            month,
                            (-6) - (
                                datediff(
                                    'month',
                                    fo.became_customer_at_contact,
                                    fo.sourced_at
                                )
                            ),
                            date_trunc('month', getdate())
                        )
                    )
                    and date_trunc('month', dcom.became_customer_at)
                    >= date_trunc('month', date_add('month', -30, getdate()))
                    and date_trunc('month', dcom.became_customer_at)
                    < date_trunc('month', date_add('month', -0, getdate()))
                    and is_sourced
                    and (datediff('month', fo.sourced_at, current_date) > 0)
                group by 1, 2, 3
                order by 2, 3, 1
            )
        select
            cc.first_closed_order_technology,
            cc.region,
            cc.cohort,
            cc.running_sum,
            sum(precalculated_margin * 1.0 / active_contacts) as sales_contact,
            sum(precalculated_margin * 1.0) as precalculated_margin,
            sum(active_contacts * 1.0 / cc.running_sum) as retention,
            retention * sales_contact as clv,
            sum(clv * 0.93) over (
                partition by cc.first_closed_order_technology, cc.region
                order by cc.cohort desc
                rows between 23 preceding and current row
            ) as clv_24m
        from cohort_contacts cc
        left join
            cohort_sales cs
            on cs.cohort = cc.cohort
            and cs.first_closed_order_technology = cc.first_closed_order_technology
            and cs.region = cc.region
        group by 1, 2, 3, 4
        order by 1 desc, 2, 3
    ),

    mql_pred as (
        select hubspot_contact_id,
               predicted_proba,
               row_number() over (partition by hubspot_contact_id order by model_executed_at asc) as row
        from {{ source('int_analytics', 'mql_conversion_pred') }}  pred
    ),
    contacts_prep as (
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

        from {{ ref("dim_contacts") }})
select
    dc.hubspot_contact_id,
    dc.became_mql_at,
    dc.mql_technology,
    dc.region,
    dc.contact_type,
    advertising_gclid,
    advertising_account_id,
    advertising_click_date,
    clv.clv_24m,
    coalesce(predicted_proba*0.81, 0.0001) as is_customer_prediction,
    is_customer_prediction * clv_24m * 
    case when contact_type = 'first_company_customer' then 2.0
         else 0.67 end as cpa_price,
    is_customer_prediction * clv_24m as original_cpa_price
from contacts_prep dc
left join mql_pred pred
    on dc.hubspot_contact_id = pred.hubspot_contact_id and pred.row = 1
left join
    clv
    on clv.first_closed_order_technology = dc.mql_technology
    and clv.region = dc.region
    and clv.cohort = 1
    where became_mql_at >= '2021-01-01' and (became_mql_at < date_add('days',-2,getdate())
    or predicted_proba is not null )

order by 1
