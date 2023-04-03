select hubspot_company_id,
        -- Calculate aov
        (select sum(closed_sales_usd_company) / sum(number_of_closed_orders_company)
         from {{ ref('agg_orders_companies') }}
         where hubspot_company_id is not null)                           as aov,

        -- RFM input
        datediff(day, recent_closed_order_at_company, getdate())         as recency,
        number_of_closed_orders_company                                  as frequency,
        closed_sales_usd_company / number_of_closed_orders_company       as monetary,

        -- R score
        case
            when recency <= 90 then 4
            when recency > 90 and recency <= 180 then 3
            when recency > 180 and recency <= 365 then 2
            when recency > 365 then 1
            end                                                          as r_score,

        -- F score
        case
            when frequency >= 4 then 4
            when frequency = 3 then 3
            when frequency = 2 then 2
            when frequency = 1 then 1
            end                                                          as f_score,

        -- M score
        case
            when monetary >= aov * 2 then 4
            when monetary < aov * 2 and monetary >= aov then 3
            when monetary < aov and monetary >= aov / 2 then 2
            when monetary < aov / 2 then 1
            end                                                          as m_score,

        -- Segmentation
        case
            when r_score > 2 and f_score > 2 and m_score > 2 then 'core_value'
            when r_score > 2 and f_score > 2 and m_score <= 2 then 'small_regular'
            when r_score > 2 and f_score <= 2 and m_score > 2 then 'core_development'
            when r_score > 2 and f_score <= 2 and m_score <= 2 then 'basic_development'
            when r_score <= 2 and f_score > 2 and m_score > 2 then 'core_maintenance'
            when r_score <= 2 and f_score > 2 and m_score <= 2 then 'basic_maintenance'
            when r_score <= 2 and f_score <= 2 and m_score > 2 then 'core_churn'
            when r_score <= 2 and f_score <= 2 and m_score <= 2 then 'low_value_churn'
            end                                                          as rfm_segment
 from {{ ref('agg_orders_companies') }}
 where recent_closed_order_at_company is not null