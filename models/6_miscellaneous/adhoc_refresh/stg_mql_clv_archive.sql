-- with
--     clv as (
        with
            cohort_companies as (
                with
                    companies as (
                        select *
                        from {{ ref('dim_companies') }}
                        where channel_grouped <> 'outbound'
                    )

                select
                    datediff('month', became_customer_at, getdate()) - 1 as cohort,
                    first_closed_order_technology,
                    region,
                    count(1) companies,
                    sum(companies) over (
                        partition by first_closed_order_technology, region
                        order by cohort desc
                        rows between 5 preceding and current row
                    ) as running_sum
                from companies
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
                        'month', fo.became_customer_at_company, fo.sourced_at
                    ) cohort,
                    dcom.first_closed_order_technology,
                    dcom.region,
                    count(distinct(fo.hubspot_company_id)) active_companies,
                    coalesce(
                        sum((subtotal_sourced_amount_usd - subtotal_sourced_cost_usd)),
                        0
                    ) as precalculated_margin
                from dbt_prod_reporting.fact_orders as fo
                left join
                    dbt_prod_reporting.dim_contacts as dc
                    on fo.hubspot_contact_id = dc.hubspot_contact_id
                left join
                    dbt_prod_reporting.dim_companies as dcom
                    on dc.hubspot_company_id = dcom.hubspot_company_id
                where
                    dcom.channel_grouped <> 'outbound'
                    and (
                        (date_trunc('month', dcom.became_customer_at)) >= dateadd(
                            month,
                            (-6) - (
                                datediff(
                                    'month',
                                    fo.became_customer_at_company,
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
            sum(precalculated_margin * 1.0 / active_companies) as sales_company,
            sum(precalculated_margin * 1.0) as precalculated_margin,
            sum(active_companies * 1.0 / cc.running_sum) as retention,
            retention * sales_company as clv,
            sum(clv * 0.93) over (
                partition by cc.first_closed_order_technology, cc.region
                order by cc.cohort desc
                rows between 23 preceding and current row
            ) as clv_24m
        from cohort_companies cc
        left join
            cohort_sales cs
            on cs.cohort = cc.cohort
            and cs.first_closed_order_technology = cc.first_closed_order_technology
            and cs.region = cc.region
        group by 1, 2, 3, 4
        order by 1 desc, 2, 3

--     mql_pred as (
--         select hubspot_company_id,
--                predicted_proba,
--                row_number() over (partition by hubspot_company_id order by model_executed_at asc) as row
--         from {{ source('int_analytics', 'mql_conversion_pred') }}  pred
--     )
-- select
--     dc.hubspot_company_id,
--     clv.clv_24m,
--     coalesce(predicted_proba,0.09) as is_customer_prediction,
--     is_customer_prediction * clv_24m as cpa_price
-- from {{ ref('dim_companies') }} dc
-- left join mql_pred pred
--     on dc.hubspot_company_id = pred.hubspot_company_id and pred.row = 1
-- left join
--     clv
--     on clv.first_closed_order_technology = dc.mql_technology
--     and clv.region = dc.region
--     and clv.cohort = 1
--     and became_mql_at >= '2021-01-01'

-- order by 1