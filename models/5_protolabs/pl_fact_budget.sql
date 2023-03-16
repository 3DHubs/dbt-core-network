select date,
       business_days,
       is_business_day,
       kpi,
       market,
       technology_name,
       integration,
       value 
from  {{ ref('fact_budget') }}