----------------------------------------------------------------
-- DEAL AGGREGATES
----------------------------------------------------------------

-- This table is built from the fact_contribution_margin table 
-- and later appended into the [fact_orders] table.

select order_uuid,
       coalesce(sum(case when (type = 'cost') then amount_usd end), 0)    as cogs_amount_usd,
       coalesce(sum(case when (type = 'revenue') then amount_usd end), 0) as recognized_revenue_amount_usd,
       coalesce(sum(amount_usd), 0)                                       as contribution_margin_amount_usd
from {{ ref('fact_contribution_margin') }}
group by 1
