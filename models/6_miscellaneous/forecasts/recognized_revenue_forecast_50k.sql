with dates as
         (select distinct year,
                          quarter,
                          month,
                          technology_name
          from {{ source("int_analytics", "dim_dates") }} d
                   cross join (select distinct fo.technology_name
                               from {{ ref("fact_orders") }} as fo
                               where technology_name IN ('SM', 'IM', 'CNC', '3DP'))
          where d.year >= '2022'
          order by 1, 2, 3, 4),
     avg_values_prep AS
         (select fo.technology_name,
                 fo.lead_time,
                 fo.order_uuid,
                 fo.promised_shipping_at_to_customer,
                 coalesce(fo.recognized_at, getdate())                                                                                           as recognized_at,
                 fo.recognized_revenue_amount_usd,
                 fo.subtotal_amount_usd,
                 is_sourced,
                 date_trunc('month', fo.promised_shipping_at_to_customer)                                                                        as month_trunc,
                 dense_rank() over (partition by fo.technology_name order by max(date_trunc('month', fo.promised_shipping_at_to_customer)) desc) as rn,
                 1                                                                                                                               as orders
          from {{ ref("fact_orders") }} as fo
                   left join
               {{ ref("fact_contribution_margin") }} as fcm on fcm.order_uuid = fo.order_uuid
          where fo.subtotal_closed_amount_usd >= 50000
            and fo.promised_shipping_at_to_customer <= dateadd(days, -7, getdate())
            and fo.promised_shipping_at_to_customer is not null
          group by 1, 2, 3, 4, 5, 6, 7, 8)
        ,
     average_values as (select avg_val.technology_name                                                                as avg_technology_name,
                               avg(avg_val.lead_time)                                                                 as avg_leadtime,
                               avg(abs(datediff(day, avg_val.promised_shipping_at_to_customer, avg_val.recognized_at)))    as avg_recognition_delay,
                               median(abs(datediff(day, avg_val.promised_shipping_at_to_customer, avg_val.recognized_at))) as median_recognition_delay,
                               stddev(abs(datediff(day, avg_val.promised_shipping_at_to_customer, avg_val.recognized_at))) as std_dev_recognition_delay,
                               median_recognition_delay::int + std_dev_recognition_delay::int                         as high_recognition_delay,
                               sum(avg_val.recognized_revenue_amount_usd)                                             as sum_amount,
                               avg(avg_val.subtotal_amount_usd)                                                       as avg_amount,
                               stddev(avg_val.recognized_revenue_amount_usd)                                          as stddev_amount,
                               avg(avg_val.recognized_revenue_amount_usd) / 6                                         as avg_monthly_amount,
                               sum(avg_val.recognized_revenue_amount_usd) / 6                                         as avg_from_sum_amount,
                               sum(orders) * 1.0 / 6                                                                  as avg_from_sum_orders,
                               sum((orders * 1.0) / 6)                                                                as avg_orders
                        from avg_values_prep as avg_val
                        where avg_val.rn <= 9     -- consider only the last n months' available data
                        group by 1)
        ,
     sold_high_expectation as (select
                                 date_trunc('month', 
                                 case when dateadd(day, 7, fo.promised_shipping_at_to_customer) < getdate() then
                                      case when dateadd(day, av.avg_recognition_delay,fo.promised_shipping_at_to_customer) < getdate() then
                                           case when dateadd(day, av.high_recognition_delay,fo.promised_shipping_at_to_customer) < getdate() then
                                            null
                                                else dateadd(day, av.high_recognition_delay, fo.promised_shipping_at_to_customer) end
                                           else dateadd(day, av.avg_recognition_delay, fo.promised_shipping_at_to_customer) end
                                      else dateadd(day, 7, fo.promised_shipping_at_to_customer) end)             as recognized_date,
                                fo.technology_name,
                                sum(fo.subtotal_sourced_amount_usd)                                              as amount,
                                '1 high'                                                                         as expectation,
                                'sold'                                                                           as type
                               from {{ ref("fact_orders") }} as fo
                                        left join average_values av on fo.technology_name = av.avg_technology_name
                               where fo.sourced_at >= date_add('year', -2, getdate())
                                 and fo.subtotal_sourced_amount_usd >= 50000
                                 and fo.recognized_at is null
                               group by 1, 2, 4, 5
                               order by 1, 2, 3) 
        ,
     sold_medium_expectation as (select 
                                   date_trunc('month', 
                                   case when dateadd(day, av.avg_recognition_delay, fo.promised_shipping_at_to_customer) < getdate() then
                                        case when dateadd(day, av.high_recognition_delay,fo.promised_shipping_at_to_customer) < getdate() then 
                                         null
                                             else dateadd(day, av.high_recognition_delay,fo.promised_shipping_at_to_customer) end
                                        else dateadd(day, av.avg_recognition_delay,fo.promised_shipping_at_to_customer) end) as recognized_date,
                                fo.technology_name,
                                sum(fo.subtotal_sourced_amount_usd)                                                          as amount,
                                '2 medium'                                                                                   as expectation,
                                'sold'                                                                                       as type
                                from {{ ref("fact_orders") }} as fo
                                          left join average_values av on fo.technology_name = av.avg_technology_name
                                where fo.sourced_at >= date_add('year', -2, getdate())
                                   and fo.subtotal_sourced_amount_usd >= 50000
                                   and fo.recognized_at is null
                                 group by 1, 2, 4, 5
                                 order by 1)
        ,
     sold_low_expectation as (select 
                                date_trunc('month', 
                                case when dateadd(day, av.high_recognition_delay,fo.promised_shipping_at_to_customer) < getdate() then 
                                 null
                                     else dateadd(day, av.high_recognition_delay,fo.promised_shipping_at_to_customer) end) as recognized_date,
                                fo.technology_name,
                                sum(fo.subtotal_sourced_amount_usd)                                                        as amount,
                                '3 low'                                                                                    as expectation,
                                'sold'                                                                                     as type

                              from {{ ref("fact_orders") }} as fo
                                       left join average_values av on fo.technology_name = av.avg_technology_name
                              where fo.sourced_at >= date_add('year', -2, getdate())
                                and fo.subtotal_sourced_amount_usd >= 50000
                                and fo.recognized_at is null
                              group by 1, 2, 4, 5
                              order by 1)
        ,
     sold_summary_prep_1 as
         (select *
          from sold_high_expectation
          union all
          select *
          from sold_medium_expectation
          union all
          select *
          from sold_low_expectation)
        ,
     sold_summary_prep as (select date_trunc('month', recognized_date)                           as recognized_date,
                                  sum(case when expectation = '1 high' then amount else 0 end)   as _1_high,
                                  sum(case when expectation = '2 medium' then amount else 0 end) as _2_medium,
                                  sum(case when expectation = '3 low' then amount else 0 end)    as _3_low,
                                  type
                           from sold_summary_prep_1
                           group by 1, 5
                           order by 1)
        ,
     sold_summary as (select recognized_date,
                             0                                                                                                        as base,
                             sum(case when _1_high > abs((_2_medium + _3_low)) then abs((_1_high - (_2_medium + _3_low))) else 0 end) as _1_high,
                             sum(case when _2_medium > abs(_3_low) then abs((_2_medium - _3_low)) else 0 end)                         as _2_medium,
                             sum(_3_low)                                                                                              as _3_low,
                             type
                      from sold_summary_prep
                      group by 1, 2, 6
                      order by 1)
        ,
     runrate as (select d.date,
                        technology_name,
                        lead_time,
                        sum(subtotal_sourced_amount_usd) / 6                                                                          as run_rate_sourced_amount
                 from {{ ref("fact_orders") }} as fo
                          cross join (select distinct date_trunc('month', d.date) as date
                                      from {{ source("int_analytics", "dim_dates") }} d
                                      where d.date >= getdate()
                                        and d.date < date_add('months', 3, date_trunc('month', getdate()))) d
                 where subtotal_sourced_amount_usd >= 50000
                   and promised_shipping_at_to_customer >= date_add('month', -6, date_trunc('months', getdate()))
                   and promised_shipping_at_to_customer < date_trunc('months', getdate())
                 group by 1, 2, 3
                 order by 1, 2, 3)
        ,
     high_f as (select technology_name,
                       case when date_add('day', 7 + lead_time + 14, date) < getdate() then
                            case when date_add('day', av.avg_recognition_delay,date_add('day', 7 + lead_time + 14, date)) < getdate() then
                                 case when dateadd(day, av.high_recognition_delay,date_add('day', 7 + lead_time + 14, date)) < getdate() then
                                  null
                                      else dateadd(day, av.high_recognition_delay, date_add('day', 7 + lead_time + 14, date)) end
                                 else dateadd(day, av.high_recognition_delay, date_add('day', 7 + lead_time + 14, date)) end
                            else date_add('day', 7 + lead_time + 14, date) end                                          as recognized_date,
                       sum(run_rate_sourced_amount)                                                                     as forecast_amount,
                       '1 high'                                                                                         as expectation,
                       'forecast'                                                                                       as type
                from runrate r
                         left join average_values av on r.technology_name = av.avg_technology_name
                group by 1, 2, 4, 5)
        ,
     medium_f as (select technology_name,
                         case when date_add('day', av.avg_recognition_delay, date_add('day', 7 + lead_time + 14, date)) < getdate() then
                              case when dateadd(day, av.high_recognition_delay, date_add('day', 7 + lead_time + 14, date)) < getdate() then
                               null
                                   else dateadd(day, av.high_recognition_delay, date_add('day', 7 + lead_time + 14, date)) end
                              else date_add('day', av.avg_recognition_delay, date_add('day', 7 + lead_time + 14, date)) end        as recognized_date,
                         sum(run_rate_sourced_amount)                                                                              as forecast_amount,
                         '2 medium'                                                                                                as expectation,
                         'forecast'                                                                                                as type
                  from runrate r
                           left join average_values av on r.technology_name = av.avg_technology_name
                  group by 1, 2, 4, 5)
        ,
     low_f as (select technology_name,
                      case when dateadd(day, av.high_recognition_delay, date_add('day', 7 + lead_time + 14, date)) < getdate() then 
                       null
                           else dateadd(day, av.high_recognition_delay, date_add('day', 7 + lead_time + 14, date)) end             as recognized_date,
                      sum(run_rate_sourced_amount) forecast_amount,
                      '3 low'                                                                                                      as expectation,
                      'forecast'                                                                                                   as type
               from runrate r
                        left join average_values av on r.technology_name = av.avg_technology_name
               group by 1, 2, 4, 5),
     forecast_summary_prep_1 as
         (select *
          from high_f
          union all
          select *
          from medium_f
          union all
          select *
          from low_f)
        ,
     forecast_summary_prep as (select date_trunc('month', recognized_date)                                    as recognized_date,
                                      sum(case when expectation = '1 high' then forecast_amount else 0 end)   as _1_high,
                                      sum(case when expectation = '2 medium' then forecast_amount else 0 end) as _2_medium,
                                      sum(case when expectation = '3 low' then forecast_amount else 0 end)    as _3_low,
                                      type
                               from forecast_summary_prep_1
                               group by 1, 5
                               order by 1)
        ,
     forecast_summary as (select recognized_date,
                                 0                                                                                                        as base,
                                 sum(case when _1_high > abs((_2_medium + _3_low)) then abs((_1_high - (_2_medium + _3_low))) else 0 end) as _1_high,
                                 sum(case when _2_medium > abs(_3_low) then abs((_2_medium - _3_low)) else 0 end)                         as _2_medium,
                                 sum(_3_low)                                                                                              as _3_low,
                                 type
                          from forecast_summary_prep
                          group by 1, 2, 6
                          order by 1),
     base_forecast as (select date_trunc('month', estimated_recognized_at) as recognized_date,
                       sum(recognized_amount)                              as base,
                       0                                                   as _1_high,
                       0                                                   as _2_medium,
                       0                                                   as _3_low,
                       'base'                                              as type
          from {{ ref('recognized_revenue_forecast') }}
          group by 1, 3, 4, 5, 6
          order by 1, 2)

select base.*
from base_forecast base
union all
select *
from forecast_summary
union all
select *
from sold_summary
order by 1, 6
