{{ config(
    tags=["multirefresh"]
) }}

with date_year_week as (
          select
              distinct CONVERT(varchar(10), dates.year * 100 + dates.week) as year_week
          from {{ source('data_lake', 'dim_dates') }} as dates
          where dates.date < DATEADD(month, 2, GETDATE())
          and dates.date > 201901
      ), supplier_options as (
          select distinct fact_ord.supplier_id as supplier_id,
                          isnull(fact_ord.destination_country, 'null') as destination_country,
                          isnull(line.material_subset_name, 'null') as material_subset_name,
                          isnull(line.surface_finish_name, 'null') as surface_finish_name,
                          isnull(line.tiered_tolerance, 'null') as tiered_tolerance

          from {{ ref('fact_orders') }} as fact_ord
          left join {{ ref('fact_line_items') }} as line on fact_ord.order_uuid = line.order_uuid
          where fact_ord.supplier_id is not null
          and fact_ord.technology_name = 'CNC'
      ), cross_year_week_supplier_options as (
          select *
          from date_year_week dates
          cross join supplier_options
      ), history_disputes as (
          select (TO_CHAR(DATE_TRUNC('week', fact_ord.derived_delivered_at), 'YYYYWW')) as year_week,
                 fact_ord.supplier_id                                                   as supplier_id,
                 isnull(fact_ord.destination_country, 'null')                           as destination_country,
                 isnull(line.tiered_tolerance, 'null')                                  as tiered_tolerance,
                 isnull(line.material_subset_name, 'null')                              as material_subset_name,
                 isnull(line.surface_finish_name, 'null')                               as surface_finish_name,
                 count(line.line_item_uuid)                                             as number_line_item,
                 sum(case
                         when line.is_dispute and
                              line.dispute_created_at <= (fact_ord.derived_delivered_at + interval '14 days')
                             then 1
                         else 0
                     end)                                                               as number_disputes_within_14_window
          from {{ ref('fact_orders') }} as fact_ord
                   left join {{ ref('fact_line_items') }} as line on fact_ord.order_uuid = line.order_uuid
          where fact_ord.derived_delivered_at is not null
          and fact_ord.technology_name = 'CNC'
          group by 1, 2, 3, 4, 5, 6
      ), supplier_destination_history as (
          select cywso.year_week,
                 cywso.supplier_id,
                 cywso.destination_country,
                 cywso.material_subset_name,
                 cywso.surface_finish_name,
                 cywso.tiered_tolerance,
                 isnull(hd.number_line_item, 0)                 as number_line_item,
                 isnull(hd.number_disputes_within_14_window, 0) as number_disputes_within_14_window

          from cross_year_week_supplier_options cywso
                   left join history_disputes as hd on cywso.year_week = hd.year_week
              and cywso.supplier_id = hd.supplier_id
              and cywso.destination_country = hd.destination_country
              and cywso.material_subset_name = hd.material_subset_name
              and cywso.surface_finish_name = hd.surface_finish_name
              and cywso.tiered_tolerance = hd.tiered_tolerance
          order by year_week desc, number_line_item desc
      ), supplier_order_dispute_history as (
      --supplier_order_dispute_history
          select sdh.year_week,
                 sdh.supplier_id,
                 sdh.material_subset_name,
                 sdh.surface_finish_name,
                 sdh.tiered_tolerance,
                 sum(sdh.number_line_item)                                                                              as number_line_item_,
                 sum(sdh.number_disputes_within_14_window)                                                              as number_disputes_within_14_window_,
                 SUM(number_line_item_) OVER (PARTITION BY sdh.supplier_id, sdh.material_subset_name,
                     sdh.surface_finish_name, sdh.tiered_tolerance ORDER BY sdh.year_week asc
                     ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)                                                          as line_item_count_last_5w,
                 SUM(number_disputes_within_14_window_) OVER (PARTITION BY sdh.supplier_id, sdh.material_subset_name,
                     sdh.surface_finish_name, sdh.tiered_tolerance ORDER BY sdh.year_week asc
                     ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)                                                          as line_item_disputes_last_5w,

                 SUM(number_line_item_) OVER (PARTITION BY sdh.supplier_id, sdh.material_subset_name,
                     sdh.surface_finish_name, sdh.tiered_tolerance ORDER BY sdh.year_week asc ROWS UNBOUNDED PRECEDING) as
                                                                                                                           running_line_item_count,
                 SUM(number_disputes_within_14_window_) OVER (PARTITION BY sdh.supplier_id, sdh.material_subset_name,
                     sdh.surface_finish_name, sdh.tiered_tolerance ORDER BY sdh.year_week asc ROWS UNBOUNDED PRECEDING) as
                                                                                                                           running_disputes_count,
                 round(nullif(line_item_disputes_last_5w * 1.00, 0) / nullif(line_item_count_last_5w, 0),
                       2)                                                                                               as dispute_percentage_last_5w,
                 round(nullif(running_disputes_count * 1.00 , 0) / nullif(running_line_item_count, 0),
                       2)                                                                                               as dispute_percentage_running
          from supplier_destination_history as sdh
          group by 1, 2, 3, 4, 5
      ), supplier_dispute_rate as (
          select sodh.year_week                                                                        as year_week,
                 sodh.supplier_id                                                                      as supplier_id,
                 sum(line_item_count_last_5w)                                                          as line_item_count_last_5w_,
                 sum(line_item_disputes_last_5w)                                                       as line_dispute_count_last_5w_,
                 round(nullif(line_dispute_count_last_5w_ * 1.00, 0) / nullif(line_item_count_last_5w_,0),
                       2)                                                                              as dispute_percentage_last_5w
          from supplier_order_dispute_history as sodh
          group by 1, 2
      ), country_order_dispute_history as (
          select hd.year_week,
                 hd.destination_country,
                 sum(hd.number_line_item)                                    as number_line_item_,
                 sum(hd.number_disputes_within_14_window)                    as number_disputes_within_14_window_,
                 SUM(number_line_item_) OVER (PARTITION BY destination_country ORDER BY year_week asc
                     ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)               as line_item_count_last_5w,
                 SUM(number_disputes_within_14_window_) OVER (PARTITION BY destination_country ORDER BY
                     year_week asc ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) as line_item_disputes_last_5w,
                 round(nullif(line_item_disputes_last_5w * 1.00, 0.01) / line_item_count_last_5w,
                       2)                                                    as dispute_percentage_last_5w
          from history_disputes as hd
          group by 1, 2
      ), freshdesk_interactions_history as (
          select fact_orders.order_uuid,
                 fact_orders.order_hubspot_deal_id,
                 count(fact_interactions.interaction_id) as interaction_count,
                 sum(case
                         when fact_interactions.source = 'Freshdesk' then 1
                         else 0
                     end)                                as freshdesk_count
          from {{ ref('fact_orders') }} fact_orders
                   left join {{ ref('fact_interactions') }} as fact_interactions
                             on fact_orders.order_hubspot_deal_id = fact_interactions.hubspot_deal_id
          where true
            and fact_orders.order_shipped_at > fact_interactions.created_at
          group by 1, 2
      ),

smart_qc as
(

      select
          fact_ord.order_uuid as order_uuid,
          fact_line.line_item_uuid as line_item_uuid,
          isnull(sdr.dispute_percentage_last_5w >= 0.09 or -- supplier dispute % last 5 weeks
              sodh.dispute_percentage_last_5w >= 0.09 or -- supplier disputes % similar orders last 5 weeks
              sodh.dispute_percentage_running >= 0.03 or -- supplier disputes % historic orders last 5 weeks
              codh.dispute_percentage_last_5w >= 0.09 or -- destination country disputes % historic orders last 5 weeks
              fih.freshdesk_count >= 36 or -- freshdesk interaction related to the order
              sodh.running_line_item_count < 10 or  -- supplier number of similar orders produced
              fact_line.line_item_price_amount_usd > 600 or -- supplier line item price amount
              (fact_line.custom_tolerance is not null or fact_line.tiered_tolerance is not null) or -- has_exceeded_standard_tolerances
              isnull(fact_line.is_cosmetic, False) = True or -- is_cosmetic
              isnull(fact_line.has_customer_note, False) = True or -- has_customer_note
              fact_ord.technology_name != 'CNC' -- non cnc is always QCed regardless of conditions
              , False) and
              (suppliers.country_code in ('cn', 'in') or suppliers.continent = 'eu') --  only for orders from cn in or eu
              as should_qc_bool,

            (
            CASE WHEN fact_line.line_item_price_amount_usd > 600 THEN '1' ELSE '0' END + -- rank 1
            CASE WHEN fact_line.custom_tolerance IS NOT NULL OR fact_line.tiered_tolerance IS NOT NULL THEN '1' ELSE '0' END + -- rank 2
            CASE WHEN  sodh.running_line_item_count < 10 THEN '1' ELSE '0' END + -- rank 3
            CASE WHEN  sodh.dispute_percentage_running >= 0.03 THEN '1' ELSE '0' END + -- rank 4
            CASE WHEN  fih.freshdesk_count >= 36 THEN '1' ELSE '0' END + -- rank 5
            CASE WHEN sodh.dispute_percentage_last_5w >= 0.09 THEN '1' ELSE '0' END + -- rank 6
            CASE WHEN ISNULL(fact_line.is_cosmetic, FALSE) = TRUE THEN '1' ELSE '0' END + -- rank 7
            CASE WHEN ISNULL(fact_line.has_customer_note, FALSE) = TRUE THEN '1' ELSE '0' END + -- rank 8
            CASE WHEN  sdr.dispute_percentage_last_5w >= 0.09 THEN '1' ELSE '0' END + -- rank 9
            CASE WHEN codh.dispute_percentage_last_5w >= 0.09 THEN '1' ELSE '0' END -- rank 10
            )
            as smarter_qc,
          suppliers.country_code,
          suppliers.continent


      from {{ ref('fact_orders') }} fact_ord
      left join {{ ref('dim_suppliers') }} suppliers on fact_ord.supplier_id = suppliers.supplier_id
      left join {{ ref('fact_line_items') }} AS fact_line on fact_ord.order_uuid = fact_line.order_uuid
      left join supplier_dispute_rate as sdr
          on (TO_CHAR(DATE_TRUNC('week', fact_ord.order_shipped_at), 'YYYYWW')) =  sdr.year_week
                 and fact_ord.supplier_id = sdr.supplier_id
      left join supplier_order_dispute_history as sodh
          on (TO_CHAR(DATE_TRUNC('week', fact_ord.order_shipped_at), 'YYYYWW')) =  sodh.year_week
                 and fact_ord.supplier_id = sodh.supplier_id
                 and isnull(fact_line.tiered_tolerance, 'null') = sodh.tiered_tolerance
                 and isnull(fact_line.material_subset_name, 'null') = sodh.material_subset_name
                 and isnull(fact_line.surface_finish_name, 'null') = sodh.surface_finish_name
      left join country_order_dispute_history as codh
          on (TO_CHAR(DATE_TRUNC('week', fact_ord.order_shipped_at), 'YYYYWW')) =  codh.year_week
                 and fact_ord.destination_country = codh.destination_country
      left join freshdesk_interactions_history as fih on fact_ord.order_uuid = fih.order_uuid
      where cast(fact_ord.order_shipped_at as date) > '2020-01-01'
      and fact_ord.supplier_id is not null
      and (TO_CHAR(DATE_TRUNC('week', fact_ord.order_shipped_at), 'YYYYWW')) is not null
      order by (TO_CHAR(DATE_TRUNC('week', fact_ord.order_shipped_at), 'YYYYWW')) desc
),

smarter_qc_dispute_percentage as

(
SELECT
    fact_line_items.is_dispute,
    smart_qc.smarter_qc ,
    count(smart_qc.smarter_qc),
    100.0 * COUNT(smart_qc.smarter_qc) / SUM(COUNT(smart_qc.smarter_qc)) OVER(PARTITION BY smart_qc.smarter_qc) AS dispute_rate

from {{ ref('fact_orders') }}  AS fact_orders
LEFT JOIN {{ ref('fact_quote_line_items') }} AS fact_line_items ON fact_line_items.order_uuid = fact_orders.order_uuid
LEFT JOIN smart_qc ON fact_line_items.line_item_uuid = smart_qc.line_item_uuid

WHERE
    (fact_orders.is_cross_docking )
    AND ( fact_orders.order_shipped_at  ) >= ((DATEADD(month,-12, DATE_TRUNC('quarter', DATE_TRUNC('quarter', DATE_TRUNC('day',GETDATE()))) )))
    AND (fact_line_items.line_item_type ) = 'part'
    and should_qc_bool = 'true'
    and where (lower(smart_qc.country_code) in ('cn', 'in') or lower(smart_qc.continent) = 'eu')


GROUP BY
    1,
    2
ORDER BY
    4 DESC,
    2 desc
)

select

smart_qc.order_uuid,
smart_qc.line_item_uuid,
smart_qc.should_qc_bool,
smart_qc.smarter_qc,
sqc.dispute_rate


from smart_qc
left join (select * from smarter_qc_dispute_percentage where is_dispute = true) as sqc on smart_qc.smarter_qc = sqc.smarter_qc
