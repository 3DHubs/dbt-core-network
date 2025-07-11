{{
    config(
        sort = "review_id",
        tags=["multirefresh"]
    )
}}


        -- Only select deals that have entered a in-review stage and create an index for all in-review changes (i.e.
        -- give every in-review change a number).
with in_review_deals as (
            select dh.deal_id,
                    dh.dealstage_mapped,
                    dh.changed_at,
                    case when dh.dealstage_mapped like 'In review%' then true else false end      as
                                                                                                    has_in_review_stage,
                    row_number() over (partition by dh.deal_id order by changed_at asc nulls last)   as total_index,
                    case
                        when dh.dealstage_mapped like 'In review - New%' then 'New'
                        when dh.dealstage_mapped like 'In review - Ongoing%' then 'Ongoing'
                        when dh.dealstage_mapped like 'In review - Completed%' then 'Completed'
                        when dh.dealstage_mapped like 'In review - Rejected%' then 'Rejected' end as review_dealstage,
                    case when review_dealstage = 'New' then
                    row_number() over (partition by dh.deal_id,review_dealstage  order by changed_at asc nulls last) else null end  as new_index,
                    time_in_stage_minutes                                                         as time_in_review_stage_minutes,
                    time_in_stage_business_minutes                                                as time_in_review_stage_business_minutes
            from  {{ ref('hubspot_deal_dealstage_history') }}  dh
            left join {{ ref('stg_hubspot_deal_reviews') }} bm on bm.primary_key = dh.primary_key
            where has_in_review_stage
        ),
        -- Then create an rfq_idx that will be used as the primary key for this table. The rfq index will be the same
        -- for every unique review process.
        review_id as (
            select rd.deal_id,
                    rd.changed_at,
                    rd.dealstage_mapped,
                    rd.total_index,
                    rd.time_in_review_stage_minutes,
                    rd.time_in_review_stage_business_minutes,
                    last_value(new_index ignore nulls) over (partition by deal_id order by changed_at rows unbounded preceding) as review_iteration,
                    concat(rd.deal_id, lpad(review_iteration, 2, 00))::bigint as review_id
            from in_review_deals rd
        ),
        -- Select timestamps for each review stage
        ts_events as (
            select review_id,
                    deal_id,
                    case when dealstage_mapped like 'In review - New%' then changed_at end       as review_new_date,
                    case when dealstage_mapped like 'In review - Ongoing%' then changed_at end   as review_ongoing_date,
                    case when dealstage_mapped like 'In review - Completed%' then changed_at end as review_completed_date,
                    case when dealstage_mapped like 'In review - Rejected%' then changed_at end  as review_rejected_date,
                    case
                        when dealstage_mapped like 'In review - New%'
                            then time_in_review_stage_minutes end                                as time_in_review_stage_new_minutes,
                    case
                        when dealstage_mapped like 'In review - Ongoing%'
                            then time_in_review_stage_minutes end                                as time_in_review_stage_ongoing_minutes,
                    case
                        when dealstage_mapped like 'In review - Completed%'
                            then time_in_review_stage_minutes end                                as time_in_review_stage_completed_minutes,
                    case
                        when dealstage_mapped like 'In review - Rejected%'
                            then time_in_review_stage_minutes end                                as time_in_review_stage_rejected_minutes,
                    case
                        when dealstage_mapped like 'In review - New%'
                            then time_in_review_stage_business_minutes end                                as time_in_review_stage_new_business_minutes,
                    case
                        when dealstage_mapped like 'In review - Ongoing%'
                            then time_in_review_stage_business_minutes end                                as time_in_review_stage_ongoing_business_minutes,
                    review_iteration
            from review_id
        ),
        -- Reduce this down to one row per RFQ id by selected the first review dates for every RFQ
        first_review_dates as (
            select review_id,
                    deal_id,
                    min(review_new_date)                      as first_review_new_date,
                    min(review_ongoing_date)                  as first_review_ongoing_date,
                    min(review_completed_date)                as first_review_completed_date,
                    min(review_rejected_date)                 as first_review_rejected_date,
                    sum(time_in_review_stage_new_minutes)     as total_time_in_review_stage_new_minutes,
                    sum(time_in_review_stage_ongoing_minutes) as total_time_in_review_stage_ongoing_minutes,
                    sum(time_in_review_stage_new_business_minutes)     as total_time_in_review_stage_new_business_minutes,
                    sum(time_in_review_stage_ongoing_business_minutes) as total_time_in_review_stage_ongoing_business_minutes,
                    review_iteration
            from ts_events
            group by 1, 2, review_iteration
        )
    select review_id::bigint                                                         as review_id,
           rfd.deal_id,
           hd.createdate                                                             as hubspot_create_date,
           rfd.first_review_new_date,
           rfd.first_review_ongoing_date,
           coalesce(rfd.first_review_rejected_date, rfd.first_review_completed_date) as review_completed_date,
           case
               when rfd.first_review_rejected_date is not null then 'rejected'
               when rfd.first_review_completed_date is not null then 'completed' end as review_result,
           rfd.total_time_in_review_stage_new_minutes,
           rfd.total_time_in_review_stage_ongoing_minutes,
           rfd.total_time_in_review_stage_new_business_minutes,
           rfd.total_time_in_review_stage_ongoing_business_minutes,
           hd.review_owner,
           review_iteration
    from first_review_dates rfd
            left join {{ ref('hubspot_deals') }} hd using (deal_id)
    order by review_id
