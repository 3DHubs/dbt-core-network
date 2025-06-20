
SELECT active,
        description,
        email,
        id,
        language,
        time_zone,
        created_at,
        updated_at,
        csat_rating,
        preferred_source,
        unique_external_id,
        load_timestamp,
        name

FROM {{ ref('dbt_src_external', "gold_airbyte_freshdesk_contacts") }}
