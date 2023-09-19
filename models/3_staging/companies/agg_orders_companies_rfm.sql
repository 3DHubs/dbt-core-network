--------------------------------
---     RFM Segmentation     ---
--------------------------------

-- Created by: XiaoHan Li 20230403
-- This model assigns RFM scores to each company, to achieve segmentation based on customer behaviour.
-- I segmented our customer base into 4 groups and 8 segments.
-- 4 Groups: Regular / Maintenance / Development / Churn
-- 8 Segments: the 4 groups each split into 2 based on AOV.
-- I defined the segmentation criteria for the RFM scores based on my initial business assessment.
-- Empirical feedback could be considered to re-shape the criteria if necessary.

select hubspot_company_id,
        -- Calculate aov
         (
         select coalesce(sum(subtotal_closed_amount_usd), 0) / nullif(count(case when is_closed then 1 else null end), 0)
         from {{ ref('stg_fact_orders') }}
         where closed_at >= dateadd(month, -12, getdate())
            and subtotal_closed_amount_usd != 0
            and hubspot_company_id is not null
            )                         as aov,

        -- RFM input
        datediff(day, recent_closed_order_at_company, getdate())         as recency,
        number_of_closed_projects_company                                as frequency,
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
            when r_score > 2 and f_score > 2 and m_score > 2 then '1 Core Value'
            when r_score > 2 and f_score > 2 and m_score <= 2 then '4 Small Regular'
            when r_score > 2 and f_score <= 2 and m_score > 2 then '2 Core Development'
            when r_score > 2 and f_score <= 2 and m_score <= 2 then '6 Basic Development'
            when r_score <= 2 and f_score > 2 and m_score > 2 then '3 Core Maintenance'
            when r_score <= 2 and f_score > 2 and m_score <= 2 then '7 Basic Maintenance'
            when r_score <= 2 and f_score <= 2 and m_score > 2 then '5 Core Churn'
            when r_score <= 2 and f_score <= 2 and m_score <= 2 then '8 Low Value Churn'
            end                                                          as rfm_segment
 from {{ ref('agg_orders_companies') }}
 where recent_closed_order_at_company is not null and monetary is not null