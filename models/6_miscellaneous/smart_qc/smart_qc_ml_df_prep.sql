with 

    job_ml_geo_prep as (
    select
        geo.model_id,
        geo.labels,
        row_number() over (partition by geo.model_id order by geo.updated desc) as row_num
    from {{ source('int_model_repo_raw', 'job_ml_geo') }} geo
),

job_ml_geo as (
    select
        upload.id as upload_id,
        geo_labels.assembly,
        geo_labels.clamp_difficulty,
        geo_labels.complexity,
        geo_labels.professionality,
        geo_labels.latheability,
        geo_labels.rigidity_problems,
        geo_labels.three_axis
    from job_ml_geo_prep geo
    cross join geo.labels geo_labels
    left join {{ source('fed_model_repo', 'upload') }} upload
        on geo.model_id = upload.model_id
    where geo.row_num = 1
)


    --FEATURES LIST
    select distinct
        fact_orders.derived_delivered_at,
        coalesce(
            case
                when fact_line_items.is_complaint and fact_line_items.complaint_is_conformity_issue then true
                when fact_line_items.is_dispute then true
                else false
            end, false)                                                                                                as line_has_issue,
        fact_line_items.line_item_uuid                                                                                 as line_item_uuid,
        coalesce (fact_line_items.is_cosmetic, false)                                                                  as is_cosmetic,                        
        coalesce (fact_orders.has_winning_bid_countered_on_price,false)                                                as has_winning_bid_countered_on_price,
        coalesce (((fact_orders.destination_region in ('emea') 
                    and fact_orders.destination_country != 'United Kingdom')
                    and (fact_orders.origin_region in ('emea') 
                    and fact_orders.origin_country != 'United Kingdom')) 
                    or (fact_orders.destination_country = 'United States'
                    and fact_orders.origin_country = 'United States'),false)                                           as order_is_locally_sourced,
        coalesce(fact_orders.lead_time, 0)                                                                             as lead_time,
        coalesce(fact_orders.number_of_part_line_items, 0)                                                             as number_of_part_line_items,
        fact_line_items.surface_finish_name,
        fact_line_items.tiered_tolerance,
        fact_orders.origin_country,
        coalesce (fact_line_items.has_customer_note,false)                                                             as has_customer_note,
        fact_orders.destination_country,
        fact_line_items.part_volume_cm3                                                                                as part_volume,
        coalesce (fact_line_items.has_threads,false)                                                                   as has_threads,
        coalesce (fact_line_items.has_fits,false)                                                                      as has_fits,
        fact_line_items.quantity,
        fact_line_items.line_item_price_amount_usd/fact_line_items.quantity                                            as price_per_part,

        ----ml geo fields
        geo.assembly,
        geo.clamp_difficulty,
        geo.complexity,
        geo.professionality,
        geo.latheability,
        geo.rigidity_problems,
        geo.three_axis,
        
        coalesce (fact_orders.derived_delivered_at between dateadd(month, -36, getdate()) and dateadd(month, -1, getdate())
            and order_status in ('completed', 'disputed'),false)                                                       as is_training_data


    from {{ ref('fact_orders') }} as fact_orders
        left join {{ ref('fact_quote_line_items') }} as fact_line_items
            on fact_orders.order_uuid = fact_line_items.order_uuid
        left join {{ ref('dim_suppliers') }} as suppliers
            on fact_orders.supplier_id = suppliers.supplier_id
        left join job_ml_geo geo on geo.upload_id = fact_line_items.upload_id


    where
        fact_orders.is_cross_docking
        and (fact_orders.derived_delivered_at >= dateadd(month, -36, getdate()) or fact_orders.derived_delivered_at is null)
        and fact_orders.technology_name = 'CNC'
        and fact_line_items.line_item_type = 'part'
        and fact_orders.is_sourced


