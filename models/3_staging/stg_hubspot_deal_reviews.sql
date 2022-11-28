-- Data is relatively slow to generate, hence an incremental load.
{{ 
	config(materialized='incremental',
    tags=["slow_running"]
    )
}}
with 

reviews as (
    select hh.*,
           coalesce(me.office_location,'Amsterdam') as office_location
    from  {{ ref ('hubspot_deal_dealstage_history') }}  hh
    left join {{ ref('hubspot_deals') }} as hs on hs.deal_id = hh.deal_id
    left join {{ ref ('hubspot_owners') }} as me on me.owner_id = hs.sales_engineer
    where next_changed_at is not null
      


{% if is_incremental() %}

	and primary_key not in (select coalesce(primary_key,'') from {{ this }})


{% endif %}
),
reviews_business_hours as (
   select 
        reviews.primary_key,
        reviews.deal_id,
        case when office_location = 'Chicago' then  convert_timezone('America/Chicago', changed_at) else
        convert_timezone('Europe/Amsterdam', changed_at) end as changed_at_local,
        case when office_location = 'Chicago' then  convert_timezone('America/Chicago', next_changed_at) else
        convert_timezone('Europe/Amsterdam', next_changed_at) end as next_changed_at_local

    from reviews
)
    select
        primary_key,
        deal_id,
        changed_at_local,
        next_changed_at_local,
        coalesce(start_business_hour.business_hour,changed_at_local)  as changed_at_local_business_hour,
        coalesce(end_business_hour.business_hour,next_changed_at_local)  as next_changed_at_local_business_hour,
        {{ business_minutes_between('changed_at_local_business_hour', 'next_changed_at_local_business_hour') }} as time_in_stage_business_minutes
    from reviews_business_hours
    left join {{ ref('business_hours') }} start_business_hour on start_business_hour.date_hour = date_trunc('hour',changed_at_local)  and start_business_hour.is_business_hour = false
    left join {{ ref('business_hours') }} end_business_hour on end_business_hour.date_hour = date_trunc('hour',next_changed_at_local)  and end_business_hour.is_business_hour = false
