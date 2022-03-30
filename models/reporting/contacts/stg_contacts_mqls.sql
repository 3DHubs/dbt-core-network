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
             select hubspot_contact_id, min(created_at) as became_cart_date
             from {{ ref('fact_orders') }}
                         group by 1
                         )
    select hc.contact_id,
           least(
               submit.mql_submit,
               upload.mql_upload,
               frontend.mql_wall_event,
               hc.hs_lifecyclestage_marketingqualifiedlead_date,
               became_cart_date) as mql_date,
               technology.name as mql_technology
    from {{ ref('stg_hs_contacts_union_legacy') }} hc
            left join submit on submit.email = hc.email
            left join upload on upload.email = hc.email
            left join frontend on frontend.email = hc.email
            left join opportunity on opportunity.hubspot_contact_id = hc.contact_id
            left join {{ ref('cnc_order_quotes') }} quotes on quotes.order_uuid =  hc.first_cart_uuid  and type='quote' and revision=1
            left join {{ ref('technologies') }} technology on technology.technology_id = quotes.technology_id
    group by 1, 2,3