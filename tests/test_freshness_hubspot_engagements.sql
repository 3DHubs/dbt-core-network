with max_created_at as (
    select
      max(created_at::date) created_at
    from {{ ref('fact_hubspot_engagements' )}} 
)
select
      created_at
    from max_created_at
where date_diff('day',created_at, getdate()) > 2 and extract(dayofweek from getdate()) not in (6,0)
limit 1