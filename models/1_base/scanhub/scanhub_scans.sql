select 
    package_scans.uuid,
    package_scans.location                                                                  as location,
    users.first_name || ' ' || users.last_name                                              as scanner_name,
    package_scans.created_at                                                                as created_at,
    package_scans.updated_at                                                                as updated_at,
    package_scans.archived_at                                                               as archived_at,

    -- Amsterdam time (CET/CEST)
        package_scans.created_at at time zone 'utc' at time zone 'europe/amsterdam'         as created_at_ams,
        package_scans.updated_at at time zone 'utc' at time zone 'europe/amsterdam'         as updated_at_ams,
        package_scans.archived_at at time zone 'utc' at time zone 'europe/amsterdam'        as archived_at_ams,

    -- localized timestamps based on city
        case 
            when package_scans.location = 'Chicago' then package_scans.created_at at time zone 'utc' at time zone 'america/chicago'
            when package_scans.location = 'Amsterdam' then package_scans.created_at at time zone 'utc' at time zone 'europe/amsterdam'
            when package_scans.location = 'Telford' then package_scans.created_at at time zone 'utc' at time zone 'europe/london'
            else package_scans.created_at -- Default to UTC if no match
        end as created_at_localized,

        case 
            when package_scans.location = 'Chicago' then package_scans.updated_at at time zone 'utc' at time zone 'america/chicago'
            when package_scans.location = 'Amsterdam' then package_scans.updated_at at time zone 'utc' at time zone 'europe/amsterdam'
            when package_scans.location = 'Telford' then package_scans.updated_at at time zone 'utc' at time zone 'europe/london'
            else package_scans.updated_at
        end as updated_at_localized,

        case 
            when package_scans.location = 'Chicago' then package_scans.archived_at at time zone 'utc' at time zone 'america/chicago'
            when package_scans.location = 'Amsterdam' then package_scans.archived_at at time zone 'utc' at time zone 'europe/amsterdam'
            when package_scans.location = 'Telford' then package_scans.archived_at at time zone 'utc' at time zone 'europe/london'
            else package_scans.archived_at 
        end as archived_at_localized,

    package_scans.order_number                                                              as document_number,
    package_scans.tracking_number,
    package_scans.is_manual,
    package_messages.message_text,
    package_messages.ship_urgency

   

from {{ source('int_airbyte_controlhub', 'package_scans') }} as package_scans
left join {{ source('int_airbyte_controlhub', 'package_messages') }} package_messages 
    on package_scans.tracking_number = package_messages.tracking_number
left join {{ ref('users') }} users on package_scans.author_id = users.user_id
