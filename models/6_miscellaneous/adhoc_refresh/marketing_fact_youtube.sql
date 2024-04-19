select view_month::date as view_date,
       video_title,
       video_publish_time::date as video_publish_date,
       impressions,
       impressions_ctr_percent*1.0/100 as impressions_ctr_percent,
       views,
       watch_time_in_hours,
       subscribers
from {{ source('ext_gsheets_v2', 'marketing_youtube') }}