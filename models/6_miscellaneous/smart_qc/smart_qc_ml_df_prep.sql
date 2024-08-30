with data as (
    with date_year_week as (
        select distinct convert(varchar(10), dates.year * 100 + dates.week) as year_week
        from {{ source('int_analytics', 'dim_dates') }} as dates
                                  where dates.date < DATEADD(month, 2, GETDATE())
                                    and dates.date > 201901
    ),

    supplier_options as (
        select distinct
            fact_orders.supplier_id,
            isnull(fact_orders.destination_country, 'null') as destination_country,
            --                           isnull(fact_orders.lead_time, 'null') as lead_time,
            isnull(line.material_subset_name, 'null')       as material_subset_name,
            isnull(line.surface_finish_name, 'null')        as surface_finish_name,
            isnull(line.tiered_tolerance, 'null')           as tiered_tolerance

        from {{ ref('fact_orders') }} as fact_orders
            left join {{ ref('fact_quote_line_items') }} as line
                on fact_orders.order_uuid = line.order_uuid
        where
            fact_orders.supplier_id is not null
            and fact_orders.technology_name = 'CNC'
    ),

    cross_year_week_supplier_options as (
        select *
        from date_year_week as dates
            cross join supplier_options
    ),

    --      select * from cross_year_week_supplier_options limit 10;
    history_disputes as (
        select
            (to_char(date_trunc('week', fact_orders.derived_delivered_at), 'YYYYWW')) as year_week,
            fact_orders.supplier_id,
            isnull(fact_orders.destination_country, 'null')                           as destination_country,             --JG COMMENT -> IS THAT RELEVANT, PERHAPS LEADTIME IS BETTER?
            --                  isnull(fact_orders.lead_time, 0)                                   as lead_time,
            isnull(line.tiered_tolerance, 'null')                                     as tiered_tolerance,
            isnull(line.material_subset_name, 'null')                                 as material_subset_name,
            isnull(line.surface_finish_name, 'null')                                  as surface_finish_name,
            count(line.line_item_uuid)                                                as number_line_item,
            sum(case
                when
                    line.is_dispute
                    and line.dispute_created_at
                    <= (fact_orders.derived_delivered_at + interval '30 days')
                    then 1
                else 0
            end)                                                                      as number_disputes_within_30_window --JG COMMENT -> RENAME TO 30 DAYS
        from {{ ref('fact_orders') }} as fact_orders
            left join {{ ref('fact_quote_line_items') }} as line
                on fact_orders.order_uuid = line.order_uuid --JG COMMENT -> FACT QUOTE LINE ITEMS
        where
            fact_orders.derived_delivered_at is not null
            and fact_orders.technology_name = 'CNC'
        group by 1, 2, 3, 4, 5, 6
    ),

    supplier_destination_history as (
        select
            cywso.year_week,
            cywso.supplier_id,
            cywso.destination_country,
            --                  cywso.lead_time,
            cywso.material_subset_name,
            cywso.surface_finish_name,
            cywso.tiered_tolerance,
            isnull(hd.number_line_item, 0)                 as number_line_item,
            isnull(hd.number_disputes_within_30_window, 0) as number_disputes_within_14_window

        from cross_year_week_supplier_options as cywso
            left join history_disputes as hd
                on
                    cywso.year_week = hd.year_week
                    and cywso.supplier_id = hd.supplier_id
                    and
                    cywso.destination_country = hd.destination_country
                    --               and cywso.lead_time = hd.lead_time
                    and cywso.material_subset_name
                    = hd.material_subset_name
                    and
                    cywso.surface_finish_name = hd.surface_finish_name
                    and cywso.tiered_tolerance = hd.tiered_tolerance
        order by year_week desc, number_line_item desc
    ),

    --select * from supplier_destination_history ORDER BY 8 DESC;
    supplier_order_dispute_history as (
        --supplier_order_dispute_history
        select
            sdh.year_week,
            sdh.supplier_id,
            sdh.material_subset_name,
            sdh.surface_finish_name,
            sdh.tiered_tolerance,
            --                  sdh.lead_time,
            sum(sdh.number_line_item)                     as number_line_item_,
            sum(
                sdh.number_disputes_within_14_window
            )                                             as number_disputes_within_14_window_,
            sum(number_line_item_) over (partition by
                sdh.supplier_id, sdh.material_subset_name,
                sdh.surface_finish_name, sdh.tiered_tolerance
            order by sdh.year_week asc
            rows between 5 preceding and current row)     as line_item_count_last_5w,
            sum(number_disputes_within_14_window_)
                over (partition by
                    sdh.supplier_id, sdh.material_subset_name,
                    sdh.surface_finish_name, sdh.tiered_tolerance
                order by sdh.year_week asc
                rows between 5 preceding and current row)
            as line_item_disputes_last_5w,

            sum(number_line_item_) over (partition by
                sdh.supplier_id, sdh.material_subset_name,
                sdh.surface_finish_name, sdh.tiered_tolerance
            order by sdh.year_week asc
            rows unbounded preceding)                     as
            running_line_item_count,
            sum(number_disputes_within_14_window_)
                over (partition by
                    sdh.supplier_id, sdh.material_subset_name,
                    sdh.surface_finish_name, sdh.tiered_tolerance
                order by sdh.year_week asc
                rows unbounded preceding)
            as
            running_disputes_count,
            round(
                nullif(line_item_disputes_last_5w * 1.00, 0) / nullif(line_item_count_last_5w, 0),
                2
            )                                             as dispute_percentage_last_5w,
            round(
                nullif(running_disputes_count * 1.00, 0) / nullif(running_line_item_count, 0),
                2
            )                                             as dispute_percentage_running
        from supplier_destination_history as sdh
        group by 1, 2, 3, 4, 5--,6
    ),

    supplier_dispute_rate as (
        select
            sodh.year_week,
            sodh.supplier_id,
            sum(line_item_count_last_5w)    as line_item_count_last_5w_,
            sum(line_item_disputes_last_5w) as line_dispute_count_last_5w_,
            round(
                nullif(line_dispute_count_last_5w_ * 1.00, 0)
                / nullif(line_item_count_last_5w_, 0),
                2
            )                               as dispute_percentage_last_5w
        from supplier_order_dispute_history as sodh
        group by 1, 2
    ),

    country_order_dispute_history as ( --JG COMMENT -> IS THIS RELEVANT?
        select
            hd.year_week,
            hd.destination_country,
            sum(hd.number_line_item)                  as number_line_item_,
            sum(hd.number_disputes_within_30_window)  as number_disputes_within_30_window_,
            sum(number_line_item_) over (
                partition by destination_country order by year_week asc
                rows between 5 preceding and current row
            )                                         as line_item_count_last_5w,
            sum(number_disputes_within_30_window_) over (partition by destination_country order by
                year_week asc
            rows between 5 preceding and current row) as line_item_disputes_last_5w,
            round(
                nullif(line_item_disputes_last_5w * 1.00, 0.01) / line_item_count_last_5w,
                2
            )                                         as dispute_percentage_last_5w
        from history_disputes as hd
        group by 1, 2
    ),

    freshdesk_interactions_total as (
        select
            fact_orders.order_uuid,
            fact_orders.order_hubspot_deal_id,
            count(fact_interactions.interaction_id) as interaction_count,
            sum(case
                when fact_interactions.source = 'Freshdesk' then 1
                else 0
            end)                                    as freshdesk_count
        from {{ ref('fact_orders') }} as fact_orders
            left join {{ ref('fact_interactions') }} as fact_interactions
                on fact_orders.order_hubspot_deal_id = fact_interactions.hubspot_deal_id
        where
            true
            and fact_orders.order_shipped_at > fact_interactions.created_at
        group by 1, 2
    )

    --FEATURES LIST
    select distinct
        fact_orders.derived_delivered_at,
        coalesce(
            case
                when
                    fact_line_items.is_complaint
                    and fact_line_items.complaint_is_conformity_issue then true
                when fact_line_items.is_dispute then true
                else false
            end, false
        )                                             as line_has_issue,

        fact_line_items.line_item_uuid              as line_item_uuid,

        coalesce (fact_line_items.is_cosmetic, false) as is_cosmetic,                          --JG COMMENT CAN WE SAY FALSE FOR ALL NULLS?
        coalesce (fact_questions.author_name is not null,
        false)                                        as order_has_question,
        coalesce (fact_orders.has_custom_finish,
        false)                                        as has_custom_finish,
        coalesce (fact_orders.has_winning_bid_countered_on_design,
        false)                                        as has_winning_bid_countered_on_design,
        coalesce (fact_orders.has_winning_bid_countered_on_price,
        false)                                        as has_winning_bid_countered_on_price,
        coalesce (((
            fact_orders.destination_region in ('emea')
            and fact_orders.destination_country != 'United Kingdom'
        )
        and (
            fact_orders.origin_region in ('emea')
            and fact_orders.origin_country != 'United Kingdom'
        ))
        or (
            fact_orders.destination_country = 'United States'
            and fact_orders.origin_country = 'United States'
        ),
        false)                                        as order_is_locally_sourced,
        coalesce(datediff(
            'day', date(fact_orders.order_shipped_at),
            date(fact_orders.promised_shipping_at_by_supplier)
        ), 0)                                         as delivery_difference_in_days_supplier, --JG COMMENT -> SUPPLIER AND CUSTOMER OPTIONS?
        coalesce(datediff(
            'day', date(fact_orders.shipped_to_customer_at),
            date(fact_orders.promised_shipping_at_to_customer)
        ), 0)                                         as delivery_difference_in_days_customer, ---- remove
        coalesce(
            fact_orders.lead_time, 0
        )                                             as lead_time,
        coalesce(
            fact_orders.number_of_part_line_items, 0
        )                                             as number_of_part_line_items,
        coalesce(
            fact_orders.number_of_rejected_responses, 0
        )                                             as number_of_rejected_responses,
        fact_line_items.line_item_price_amount_usd    as amount,

        coalesce(sdr.dispute_percentage_last_5w, 0)   as supplier_dispute_rate,                -- supplier dispute % last 5 weeks
        coalesce(sodh.dispute_percentage_last_5w, 0)  as supplier_similar_orders_dispute_rate, -- supplier disputes % similar orders last 5 weeks
        coalesce(sodh.dispute_percentage_running, 0)  as supplier_historic_order_dispute_rate, -- supplier disputes % historic orders last 5 weeks
        coalesce(codh.dispute_percentage_last_5w, 0)  as destination_country_dispute_rate,     -- destination country disputes % historic orders last 5 weeks
        coalesce(sodh.running_line_item_count, 0)     as supplier_similar_orders_produced,      -- supplier number of similar orders produced

        --fact_line_items.material_subset_name,
        fact_line_items.surface_finish_name,
        fact_line_items.tiered_tolerance,
        fact_orders.origin_country,
        coalesce (fact_line_items.has_customer_note,
        false)                                        as has_customer_note,
        fact_orders.destination_country,
        fact_line_items.part_volume_cm3               as part_volume,

        coalesce (fact_line_items.has_threads,
        false)                                        as has_threads,
        coalesce (fact_line_items.has_fits,
        false)                                        as has_fits,
        coalesce (fact_line_items.has_part_marking,
        false)                                        as has_part_markings,
        coalesce (fact_orders.derived_delivered_at between dateadd(month, -36, getdate()) and dateadd(month, -1, getdate())
        and order_status in ('completed', 'disputed'),
        false)                                        as is_training_data


    from {{ ref('fact_orders') }} as fact_orders
        left join {{ ref('fact_quote_line_items') }} as fact_line_items
            on fact_orders.order_uuid = fact_line_items.order_uuid
        left join {{ ref('dim_suppliers') }} as suppliers
            on fact_orders.supplier_id = suppliers.supplier_id
        left join
            (select
                max(author_name) as author_name,
                order_uuid
            from {{ ref('fact_questions') }}.fact_questions group by order_uuid) as fact_questions
            on fact_orders.order_uuid = fact_questions.order_uuid
        left join freshdesk_interactions_total as fact_interactions_total
            on fact_orders.order_uuid = fact_interactions_total.order_uuid
        left join supplier_dispute_rate as sdr
            on
                (to_char(date_trunc('week', fact_orders.order_shipped_at), 'YYYYWW')) = sdr.year_week
                and fact_orders.supplier_id = sdr.supplier_id
        left join supplier_order_dispute_history as sodh
            on
                (to_char(date_trunc('week', fact_orders.order_shipped_at), 'YYYYWW')) = sodh.year_week
                and fact_orders.supplier_id = sodh.supplier_id
                and isnull(fact_line_items.tiered_tolerance, 'null') = sodh.tiered_tolerance
                and isnull(fact_line_items.material_subset_name, 'null') = sodh.material_subset_name
                and isnull(fact_line_items.surface_finish_name, 'null') = sodh.surface_finish_name
        left join country_order_dispute_history as codh
            on
                (to_char(date_trunc('week', fact_orders.order_shipped_at), 'YYYYWW')) = codh.year_week
                and fact_orders.destination_country = codh.destination_country

    where
        fact_orders.is_cross_docking
        and (fact_orders.derived_delivered_at >= dateadd(month, -36, getdate()) or fact_orders.derived_delivered_at is null)
        and fact_orders.technology_name = 'CNC'
        and fact_line_items.line_item_type = 'part'
        and fact_orders.is_sourced

)

select * from data
