-----------------------------
--     SDR Commissions     --
-----------------------------
-- This model calculates the available bonus per supplier per quarter. 
-- When an SDR leaves the job, the bonus for that quarter is 0. 
-- XiaoHan Li 20230511

with seed_file_data as (
    select
            sc.sdr_looker_email,
            sc.sdr_owner,
            sc.start_at::date,
            sc.end_at::date,
            sc.onboarded_suppliers,
            split_to_array(sc.Onboarded_suppliers, ',') as supplier_array
    from {{ source('ext_gsheets', 'sdr_commissions') }} as sc
    where sc.sdr_owner != 'Fictional Account'
),

    transformed_seed_file as (
        select sfd.sdr_owner,
            sfd.sdr_looker_email,
            sfd.start_at           as sdr_start_at,     --Start date of the SDR
            sfd.end_at             as sdr_end_at,       --End date of the SDR
            cast(suppliers as int) as supplier_id
        from seed_file_data as sfd
            left join sfd.supplier_array as suppliers on true
    ),

    determine_commission_dates as (
        select tsf.*,
            ds.supplier_name,
            ds.create_date::date,                       -- Creation date of the supplier
            case
                when tsf.supplier_id = 291 then '2022-10-12'::date
                else ds.first_sourced_order end                             as first_sourced_order_at,

            TO_DATE(TO_CHAR(date_part(year, first_sourced_order_at), '0000') || '-' ||
                    TO_CHAR(date_part(quarter, first_sourced_order_at) * 3 - 2, '00') || '-01',
                    ' YYYY- MM- DD')                                        as commission_start_at,  -- First day of the Commission start quarter
            date_add('day', -1, date_add('Month', 12, commission_start_at))::date as commission_end_at -- Last day of the Commission end quarter
        from transformed_seed_file as tsf
            left join {{ ref('dim_suppliers') }} as ds on tsf.supplier_id = ds.supplier_id
    ),

    supplier_sourced_amounts as (
        select fo.supplier_id,
            fo.supplier_name,
            date_trunc('quarter', fo.sourced_at)::date as sourced_quarter,
            sum(fo.subtotal_sourced_amount_usd)  as quaterly_sourced_revenue -- Sourced amount per supplier per quarter eligible for commission
        from {{ ref('fact_orders') }} as fo
            left join determine_commission_dates as dcd on fo.supplier_id = dcd.supplier_id
        where fo.sourced_at is not null
        and dcd.supplier_id is not null
        and fo.sourced_at >= dcd.commission_start_at  -- Only the amount sourced during the commission period is calculated
        and fo.sourced_at <= dcd.commission_end_at    -- Only the amount sourced during the commission period is calculated
        -- to do: include SDR end date
        group by 1, 2, 3
    )

select row_number() over (ORDER BY ssa.sourced_quarter asc)                          AS prim_key,
        dcd.supplier_id,
        dcd.supplier_name,
        ssa.sourced_quarter,
        ssa.quaterly_sourced_revenue,
        dcd.sdr_owner,
        dcd.sdr_looker_email,
        dcd.sdr_start_at,
        dcd.sdr_end_at,
        dcd.create_date as supplier_created_date,
        dcd.first_sourced_order_at,
        dcd.commission_start_at,
        dcd.commission_end_at,
        ssa.sourced_quarter < commission_end_at                                       as eligible_for_bonus,  -- Bonus only available before commission end period
        case when ssa.sourced_quarter = dcd.commission_start_at then 50 else 0 end    as onboarding_bonus,    -- One time onboarding bonus at commission start quarter
        
        -- Calculate quarterly engagement bonus
        case when eligible_for_bonus then
            case when date_part('year', ssa.sourced_quarter) = 2022 then 
                    case when ssa.quaterly_sourced_revenue >= 60000 then 200
                        when ssa.quaterly_sourced_revenue >= 30000 then 100
                        else 0
                    end
                 when date_part('year', ssa.sourced_quarter) >= 2023 then
                    case when ssa.quaterly_sourced_revenue > 30000 then 600
                        when ssa.quaterly_sourced_revenue > 20000 then 300
                        when ssa.quaterly_sourced_revenue > 10000 then 200
                        when ssa.quaterly_sourced_revenue > 5000  then 100
                        else 0
                    end
            end
        else 0 
        end                                                                            as engagement_bonus,

        -- Calculate quarterly total bonus
        onboarding_bonus + engagement_bonus                                            as total_locked_bonus,

        -- Set payout to 0 for the quarter the SDR leaves the job and afterwards
        case when eligible_for_bonus then
            case when dcd.sdr_end_at is null then 1
                when dcd.sdr_end_at is not null then
                    case when date_trunc('quarter', dcd.sdr_end_at) > date_trunc('quarter', ssa.sourced_quarter) then 1
                        when date_trunc('quarter', dcd.sdr_end_at) <= date_trunc('quarter', ssa.sourced_quarter) then 0
                    end
            end
        else 0 
        end                                                                            as payout_availability,
        
        -- Calculate quarterly total bonus available for payout
        case when eligible_for_bonus then total_locked_bonus * payout_availability else 0 end as unlocked_bonus, 

        case when eligible_for_bonus then onboarding_bonus + 600 else 0 end           as potential_bonus,
        case when ssa.sourced_quarter = dcd.commission_start_at then 1 else 0 end     as sdr_new_suppliers_this_quarter,
        case when dcd.first_sourced_order_at is null then 1 else 0 end                as sdr_new_suppliers_not_yet_sourced,
        case when dcd.first_sourced_order_at is not null then 1 else 0 end            as sdr_new_suppliers,
        case when eligible_for_bonus then 1 else 0 end                                as sdr_suppliers_eligible_for_bonus
from determine_commission_dates as dcd
    left join supplier_sourced_amounts as ssa on dcd.supplier_id = ssa.supplier_id