with submit as (
    select email, min(date::timestamp) as mql_submit from {{ source('data_lake', 'hubspot_email_wall_submissions_20210423') }} group by 1
    ),
         upload as (
             select customer_email email, min(received_at) as mql_upload
                     from {{ source('_3d_hubs', 'supply_quote_updated') }}
                     where action = 'add'
                     group by 1
                     ),
         frontend as (
             select email, min(received_at) as mql_wall_event
                     from {{ source('_3d_hubs', 'frontend_email_wall_submitted') }}
                     group by 1
                     ),
         opportunity as (
             select hubspot_contact_id, min(order_submitted_date) as became_opportunity_date
             from {{ ref('fact_orders') }}
                         group by 1
                         )
    select hc.contact_id,
        least(submit.mql_submit, upload.mql_upload, frontend.mql_wall_event,
                hc.hs_lifecyclestage_marketingqualifiedlead_date,
                became_opportunity_date) as mql_date
    from {{ source('data_lake', 'hubspot_contacts') }} hc
            left join submit on submit.email = hc.email
            left join upload on upload.email = hc.email
            left join frontend on frontend.email = hc.email
            left join opportunity on opportunity.hubspot_contact_id = hc.contact_id
    group by 1, 2