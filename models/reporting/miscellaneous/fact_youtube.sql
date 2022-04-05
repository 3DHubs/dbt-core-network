select view_month as view_date,
       video_title,
       video_publish_time as video_publish_date,
       impressions,
       impressions_ctr_percent,
       views,
       watch_time_in_hours,
       subscribers
from {{ source('ext_gsheets', 'marketing_youtube') }}