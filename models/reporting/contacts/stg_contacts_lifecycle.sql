with agg_deal_prep as (
        select hubspot_contact_id, min(order_closed_at) as became_contact_customer_date
        from {{ ref('fact_orders') }}
        group by 1),
agg_deal as (
    select order_uuid,
           datediff('month', became_contact_customer_date, cd.order_closed_at) = 0 as is_new_contact
    from {{ ref('fact_orders') }} cd
             left join agg_deal_prep ad on ad.hubspot_contact_id = cd.hubspot_contact_id
        )

select distinct c.hs_contact_id                                          as hubspot_contact_id,
                    c.hs_company_id                                          as hubspot_company_id,
                    min(c.became_lead_date) over (partition by c.hs_contact_id)
                                                                             as became_lead_date,
                    min(c.became_sql_date) over (partition by c.hs_contact_id)
                                                                             as became_sql_date,
                    min(mql.mql_date) over (partition by c.hs_contact_id)
                                                                             as became_mql_date,
                    min(cd.order_submitted_at) over (partition by c.hs_contact_id) -- change to order_quote_submitted_at?
                                                                             as became_opportunity_date,
                    min(cd.order_closed_at) over (partition by c.hs_contact_id)
                                                                             as became_customer_date,
                    nullif(sum(cd.order_closed_amount_usd) over (partition by c.hs_contact_id), 0)
                                                                             as total_order_closed_sales_usd,
                    first_value(cd.order_technology_name)
                    over ( partition by cd.hubspot_contact_id order by cd.order_submitted_at asc rows between unbounded
                        preceding and unbounded following)                   as first_quote_technology,
                    first_value(
                    case when cd.is_closed then cd.order_technology_name end)
                    over ( partition by cd.hubspot_contact_id order by cd.is_closed desc, cd.order_closed_at asc rows
                        between unbounded preceding and unbounded following) as first_order_technology, -- returns the technology of the first order that is_closed, null when the client has no closed_won orders
                    first_value(
                    case when cd.is_closed then cd.line_item_process_name end)
                    over ( partition by cd.hubspot_contact_id order by cd.is_closed desc, cd.order_closed_at asc rows
                        between unbounded preceding and unbounded following) as first_order_process_name,
                    nth_value(case when is_closed then order_closed_at else null end, 2)
                    over ( partition by cd.hubspot_contact_id order by cd.is_closed desc, cd.order_closed_at asc rows
                        between unbounded preceding and unbounded following) as second_order_closed_at,
                    sum(case
                            when cd.is_closed and deal.is_new_contact then cd.order_closed_amount_usd end) over ( partition by
                        cd.hubspot_contact_id)                               as new_customer_order_closed_sales_usd,
                    sum(case
                            when cd.is_closed and deal.is_new_contact then (cd.order_sourced_amount_usd - cd.sourced_cost_usd)
                        end)
                    over (partition by cd.hubspot_contact_id)                as new_customer_precalc_margin_usd,
                    first_value(cd.destination_country_iso2)
                    over ( partition by cd.hubspot_contact_id order by cd.order_closed_at asc rows between unbounded preceding
                        and unbounded following)                             as first_quote_country_iso2
    from reporting.stg_dim_contacts c
             left join {{ ref('fact_orders') }} cd on c.hs_contact_id = cd.hubspot_contact_id
             left join {{ ref('stg_contacts_mqls') }} mql on mql.contact_id = c.hs_contact_id
             left join agg_deal as deal on deal.order_uuid = cd.order_uuid