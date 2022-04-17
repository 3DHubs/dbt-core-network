select date,
       business_days,
       is_business_day,
       kpi,
       market,
       technology_name,
       value 
from  {{ ref('fact_budget') }}