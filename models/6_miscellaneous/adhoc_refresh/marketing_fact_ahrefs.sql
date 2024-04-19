select backlink_month::date as backlink_date,
       country,
       technology,
       category,
       keywords,
       target_url,
       search_volume,
       avg_position,
       position_us,
       backlinks,
       cost_usd,
       notes
from {{ source('ext_gsheets_v2', 'marketing_ahrefs') }}