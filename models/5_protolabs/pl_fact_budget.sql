select date,
       business_days,
       is_business_day,
       is_business_day_us,
       kpi,
       market,
       technology_name,
       integration,
       value
       
       
from  {{ ref('fact_budget') }}
