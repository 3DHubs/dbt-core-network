with submit as (select
    email,
    min("date"::timestamp) as mql_submit
    from {{ source('data_lake', 'hubspot_email_wall_submissions_20210423') }}
    group by 1),

upload as (select
    customer_email as email,
    min(received_at) as mql_upload
    from {{ source('_3d_hubs','supply_quote_updated') }}
    where action = 'add'
    group by 1),

frontend as (select
    email,
    min(received_at) as mql_wall_event
    from {{ source('_3d_hubs', 'frontend_email_wall_submitted') }}
    group by 1),

opportunity as (
    select hubspot_contact_id,
           min(quote_submitted_date) as became_opportunity_date
    from {{ source('reporting', 'cube_deals') }}
    group by 1)

select
    data_lake_hubspot_contacts.contact_id,
    associatedcompanyid as company_id,
    least(submit.mql_submit, upload.mql_upload, frontend.mql_wall_event,
          data_lake_hubspot_contacts.hs_lifecyclestage_marketingqualifiedlead_date,
        became_opportunity_date) as mql_date
from {{ source('data_lake', 'hubspot_contacts') }} as data_lake_hubspot_contacts
left join submit on submit.email = data_lake_hubspot_contacts.email
left join upload on upload.email = data_lake_hubspot_contacts.email
left join frontend on frontend.email = data_lake_hubspot_contacts.email
left join
    opportunity on
        opportunity.hubspot_contact_id = data_lake_hubspot_contacts.contact_id
group by 1, 2, 3
