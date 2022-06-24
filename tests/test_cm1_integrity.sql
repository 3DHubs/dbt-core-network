----------------------------------------------------------------
-- CM1 - Integrity Test
----------------------------------------------------------------

-- Test use case summary:
-- Ensure that historic CM1 does not get adjusted due to code changes.
-- This is achieved by comparing the backup of CM1 on a daily basis to the comparison table (production or cloud test)

-- Last updated: Junee 22, 2022
-- Maintained by: Daniel Salazar


with backup_agg as (
    select backup_cm1.recognized_date as recognition_date,
           sum(backup_cm1.amount_usd) as cm1
    from {{ source('dbt_backups','backup_fact_contribution_margin') }} as backup_cm1
    where backup_cm1.recognized_date < date_trunc('month', GETDATE())
    group by 1
), prod_agg as (
    select comparison_cm1.recognized_date as recognition_date,
           sum(comparison_cm1.amount_usd) as cm1
    from {{ ref('fact_contribution_margin') }} as comparison_cm1
    where comparison_cm1.recognized_date < date_trunc('month', GETDATE())
    group by 1
)

select ba.recognition_date, abs(sum(ba.cm1)-sum(pa.cm1)) as difference
from backup_agg as ba
left join prod_agg as pa on ba.recognition_date = pa.recognition_date
group by 1
having difference > 1