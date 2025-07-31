-- source from Hubspot Stitch setup initially by Nihad.
select
    hc.property_createdate__value as created_at,
    hc.property_name__value as name,
    (
        case
            when nullif(hc.property_numberofemployees__value__double, '') > 23000000
            then null
            else hc.property_numberofemployees__value__double
        end
    )::int as number_of_employees,
    nullif(hc.property_industry__value, '')::varchar as industry,
    hc.companyid::bigint as hubspot_company_id,
    nullif(hc.property_country__value, '')::varchar as country,
    nullif(hc.property_city__value, '')::varchar as city,
    hc.companyid::bigint as company_id,
    
    -- todo-migration: Snowflake way of processing the UNIX timestamp, needs testing 
    to_timestamp(cast(property_attempted_to_contact_date_company__value as bigint) / 1000) as attempted_to_contact_at,

    -- todo-migration: Snowflake way of processing the UNIX timestamp, needs testing 
    to_timestamp(cast(property_connected_date_company__value as bigint) / 1000) as connected_at,

    nullif(hc.property_hubspot_owner_id__value, '')::bigint as hubspot_owner_id,
    nullif(hc.property_ae_assigned__value, '')::int as ae_id,
    
    -- todo-migration: field is not available upstream now, change when field is available
    null as ultimate_company_owner_id,
    -- nullif(hc.property_ultimate_company_owner__value, '')::bigint as ultimate_company_owner_id,

    -- todo-migration: field is not available upstream now, change when field is available
    null as ultimate_company_owner_role,
    -- nullif(hc.property_ultimate_company_owner_role__value, '')::varchar as ultimate_company_owner_role,
    
    -- todo-migration: I changed the trunc to date_trunc, to be checked.
    date_trunc('day', property_hubspot_owner_assigneddate__value) as hubspot_owner_assigned_date,
    
    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(nullif(hc.property_added_as_strategic__value, '') as bigint) / 1000) as became_strategic_date,

    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(nullif(hc.property_added_as_ae__value, '') as bigint) / 1000) as became_ae_account_date,

    hc.property_hs_lead_status__value::character varying as hs_lead_status,
    case
        when len(property_founded_year__value) < 5
        then nullif(nullif(property_founded_year__value, ''), 'N/A')::int
    end as founded_year,
    hc.property_total_money_raised__value as total_money_raised,
    case
        when hc.property_deactivated__value = 'true'
        then true
        when hc.property_deactivated__value = 'false'
        then false
    end::boolean as is_deactivated,

    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(hc.property_deactivated_date__value as bigint) / 1000) as deactivated_date,

    case
        when hc.property_reactivated_opportunity__value = 'true'
        then true
        when hc.property_reactivated_opportunity__value = 'false'
        then false
    end::boolean as is_reactivated_opportunity,

    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(hc.property_reactivated_opportunity_date__value as bigint) / 1000) as reactivated_opportunity_date,

    case
        when hc.property_reactivated_customer__value = 'true'
        then true
        when hc.property_reactivated_customer__value = 'false'
        then false
    end::boolean as is_reactivated_customer,
    case
    when LOWER(hc.property_true_outbound__value) = 'true' then true
    when LOWER(hc.property_true_outbound__value) = 'false' then false
    end::boolean as true_outbound,

    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(hc.property_reactivated_customer_date__value as bigint) / 1000) as reactivated_customer_date,

    hc.property_company_lead_score__value::int as lead_score,
    nullif(hc.property_tier__value__double, '')::int as tier,
    case
        when hc.property_qualified__value = 'true'
        then true
        when hc.property_qualified__value = 'false'
        then false
    end::boolean as is_qualified,
    case
        when hc.property_strategic__value = 'true'
        then true
        when hc.property_strategic__value = 'false'
        then false
    end::boolean as strategic,
    nullif(hc.property_inside_sales_owner__value, '') as inside_sales_owner,
    nullif(hc.property_handover_owner__value, '') as handover_owner,

    nullif(hc.property_network_sales_specialist__value, '')::bigint as network_sales_specialist_id,
    
    hc.property_notes_last_updated__value as notes_last_updated_at,
    hc.property_notes_last_contacted__value as notes_last_contacted_at,
    case
        when hc.property_outbound_handover__value = 'true'
        then true
        when hc.property_outbound_handover__value = 'false'
        then false
    end::boolean as is_outbound_handover,

    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(nullif(hc.property_outbound_handover_date__value, '') as bigint) / 1000) as outbound_handover_date,

    nullif(hc.property_winning_auction_bid__value__double, '')::int as us_sales_account_draft_winning_bid,
    nullif(hc.property_total_auction_bid__value, '')::int as us_sales_account_draft_total_bid,

    -- todo-migration: replaced interval arithmetic with to_timestamp for Snowflake
    to_timestamp(cast(nullif(hc.property_auction_date__value, '') as bigint) / 1000) as us_sales_account_draft_date,
    
    0 as is_deleted,
    case
        when hc.property_ultra_strategic__value = 'true'
        then true
        when hc.property_ultra_strategic__value = 'false'
        then false
    end::boolean as ultra_strategic
from {{ source("ext_hubspot", "companies") }} hc
