{{ config(
          pre_hook=["
            update {{ source('int_analytics', 'legacy_orders') }} 
            set hubspot_company_id = associatedcompanyid
            from {{ ref('hubspot_contacts') }} 
            where legacy_orders.hubspot_contact_id = hubspot_contacts.contact_id
        "],
            ) }}

select 1
