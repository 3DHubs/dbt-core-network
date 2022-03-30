with first_order as (
    select order_uuid,
           hubspot_company_name,
           hubspot_company_id,
           closed_at,
           became_customer_at_company,
           technology_name,
           has_technical_drawings,
           number_of_part_line_items,
           total_quantity,
           subtotal_closed_amount_usd
    from dbt_prod_reporting.fact_orders
    where became_customer_at_company = closed_at

    )
    select com.hubspot_company_id,
           com.name,
           date_trunc('year', became_customer_at)::date as became_customer_year,
           date_diff('month', became_customer_at, current_date) as company_age_months,
           industry_mapped,
           number_of_employees,
           founded_year,
           has_technical_drawings,
           technology_name,
           number_of_part_line_items,
           total_quantity,
           closed_sales_usd,
           number_of_closed_orders,
           subtotal_closed_amount_usd,
           round(btyd.alive_probability,2) as alive_probability,
           coalesce(is_strategic,false) as is_strategic,
           s.tiering_probability,
           case when sample = 'test' then v2.tiering_probability else null end as test_tiering_probability,
           v2.tiering_probability as v2_tiering_probability,
           v2.sample as sample_category
    from dbt_prod_reporting.dim_companies as com
    left join data_lake.btyd as btyd on com.hubspot_company_id = btyd.id
    left join first_order as fo on com.hubspot_company_id = fo.hubspot_company_id
    inner join  temp.segmentation s on s.hubspot_company_id = com.hubspot_company_id
    left join temp.segmentation_v2 v2 on v2.hubspot_company_id = com.hubspot_company_id and v2.snapshot_date='2022-03-25'
    where true --became_customer_at < date_add('month',-6,getdate())
    and date_trunc('week', btyd.snapshot_date) = date_trunc('week', getdate())
    and date_trunc('week', btyd.btyd_date) = date_trunc('week', getdate())