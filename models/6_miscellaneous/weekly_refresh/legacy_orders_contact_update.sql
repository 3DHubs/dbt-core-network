{{ config(bind=False,
          pre_hook=["
            update {{ source('data_lake', 'legacy_orders') }} 
            set hubspot_company_id = associatedcompanyid
            from {{ source('data_lake', 'hubspot_contacts_stitch') }}
            where legacy_orders.hubspot_contact_id = hubspot_contacts_stitch.contact_id
        "],
            ) }}

select 1
