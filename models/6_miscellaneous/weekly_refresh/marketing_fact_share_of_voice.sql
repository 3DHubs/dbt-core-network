select search_month::date as search_date,
       brand,
       seach_volume
from {{ source('ext_gsheets', 'marketing_share_of_voice') }}