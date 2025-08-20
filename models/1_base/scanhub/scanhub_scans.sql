select 
    package_scans.uuid,
    package_scans.location                                                                  as location,
    users.first_name || ' ' || users.last_name                                              as scanner_name,
    package_scans.created_at                                                                as created_at,
    package_scans.updated_at                                                                as updated_at,
    package_scans.archived_at                                                               as archived_at,

    -- Amsterdam time (CET/CEST)
    convert_timezone('Etc/UTC', 'Europe/Amsterdam', package_scans.created_at::timestamp_ntz)   as created_at_ams, --todo-migration-test convert_timezone
    convert_timezone('Etc/UTC', 'Europe/Amsterdam', package_scans.updated_at::timestamp_ntz)   as updated_at_ams, --todo-migration-test convert_timezone
    convert_timezone('Etc/UTC', 'Europe/Amsterdam', package_scans.archived_at::timestamp_ntz)  as archived_at_ams, --todo-migration-test convert_timezone

    -- localized timestamps based on city
    case 
        when package_scans.location = 'Chicago'   then convert_timezone('Etc/UTC', 'America/Chicago', package_scans.created_at::timestamp_ntz)
        when package_scans.location = 'Amsterdam' then convert_timezone('Etc/UTC', 'Europe/Amsterdam', package_scans.created_at::timestamp_ntz)
        when package_scans.location = 'Telford'   then convert_timezone('Etc/UTC', 'Europe/London', package_scans.created_at::timestamp_ntz)
        else package_scans.created_at
    end                                                                                     as created_at_localized, --todo-migration-test convert_timezone

    case 
        when package_scans.location = 'Chicago'   then convert_timezone('Etc/UTC', 'America/Chicago', package_scans.updated_at::timestamp_ntz)
        when package_scans.location = 'Amsterdam' then convert_timezone('Etc/UTC', 'Europe/Amsterdam', package_scans.updated_at::timestamp_ntz)
        when package_scans.location = 'Telford'   then convert_timezone('Etc/UTC', 'Europe/London', package_scans.updated_at::timestamp_ntz)
        else package_scans.updated_at
    end                                                                                     as updated_at_localized, --todo-migration-test convert_timezone

    case 
        when package_scans.location = 'Chicago'   then convert_timezone('Etc/UTC', 'America/Chicago', package_scans.archived_at::timestamp_ntz)
        when package_scans.location = 'Amsterdam' then convert_timezone('Etc/UTC', 'Europe/Amsterdam', package_scans.archived_at::timestamp_ntz)
        when package_scans.location = 'Telford'   then convert_timezone('Etc/UTC', 'Europe/London', package_scans.archived_at::timestamp_ntz)
        else package_scans.archived_at 
    end                                                                                     as archived_at_localized, --todo-migration-test convert_timezone

    package_scans.order_number                                                              as document_number,
    package_scans.tracking_number,
    package_scans.is_manual,
    package_messages.message_text,
    package_messages.ship_urgency

   

from {{ source('int_airbyte_controlhub', 'package_scans') }} as package_scans
left join {{ source('int_airbyte_controlhub', 'package_messages') }} package_messages 
    on package_scans.tracking_number = package_messages.tracking_number
left join {{ ref('users') }} users on package_scans.author_id = users.user_id
