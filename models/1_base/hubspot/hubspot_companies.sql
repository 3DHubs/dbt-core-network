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
    (
        timestamp 'epoch'
        + hc.property_attempted_to_contact_date_company__value
        / 1000
        * interval '1 second'
    )::timestamp without time zone as attempted_to_contact_at,
    (
        timestamp 'epoch'
        + property_connected_date_company__value / 1000 * interval '1 second'
    )::timestamp without time zone as connected_at,
    nullif(hc.property_hubspot_owner_id__value, '')::bigint as hubspot_owner_id,
    nullif(hc.property_ae_assigned__value, '')::int as ae_id,
    trunc(hc.property_hubspot_owner_assigneddate__value)::date
    as hubspot_owner_assigned_date,
    (
        timestamp 'epoch'
        + nullif(hc.property_added_as_strategic__value, '') / 1000 * interval '1 second'
    )::timestamp without time zone as became_strategic_date,
    (
        timestamp 'epoch'
        + nullif(hc.property_added_as_ae__value, '') / 1000 * interval '1 second'
    )::timestamp without time zone as became_ae_account_date,
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
    (
        timestamp 'epoch'
        + hc.property_deactivated_date__value / 1000 * interval '1 second'
    )::timestamp without time zone as deactivated_date,
    case
        when hc.property_reactivated_opportunity__value = 'true'
        then true
        when hc.property_reactivated_opportunity__value = 'false'
        then false
    end::boolean as is_reactivated_opportunity,
    (
        timestamp 'epoch'
        + hc.property_reactivated_opportunity_date__value / 1000 * interval '1 second'
    )::timestamp without time zone as reactivated_opportunity_date,
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
    (
        timestamp 'epoch'
        + hc.property_reactivated_customer_date__value / 1000 * interval '1 second'
    )::timestamp without time zone as reactivated_customer_date,
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
    hc.property_notes_last_updated__value as notes_last_updated_at,
    hc.property_notes_last_contacted__value as notes_last_contacted_at,
    case
        when hc.property_outbound_handover__value = 'true'
        then true
        when hc.property_outbound_handover__value = 'false'
        then false
    end::boolean as is_outbound_handover,
    (
        timestamp 'epoch'
        + nullif(hc.property_outbound_handover_date__value, '')
        / 1000
        * interval '1 second'
    )::timestamp without time zone
     as outbound_handover_date,
    nullif(hc.property_winning_auction_bid__value__double, '')::int as us_sales_account_draft_winning_bid,
    nullif(hc.property_total_auction_bid__value, '')::int as us_sales_account_draft_total_bid,
    (
        timestamp 'epoch'
        + nullif(hc.property_auction_date__value, '')
        / 1000
        * interval '1 second'
    )::timestamp without time zone
     as us_sales_account_draft_date,
    0 as is_deleted,
    case
        when hc.property_ultra_strategic__value = 'true'
        then true
        when hc.property_ultra_strategic__value = 'false'
        then false
    end::boolean as ultra_strategic
from {{ source("ext_hubspot", "companies") }} hc
