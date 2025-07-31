-- source from Hubspot Stitch setup initially by Nihad.
select nullif(property_lifecyclestage__value, '')::varchar(1024)                                        as lifecyclestage,
       nullif(property_country__value, '')::varchar(1024)                                               as country,
       nullif(property_lead_source__value, '')::varchar(1024)                                           as lead_source,
       nullif(property_lastname__value, '')::varchar(1024)                                              as lastname,
       nullif(property_jobtitle__value, '')::varchar(1024)                                              as jobtitle,
       nullif(property_ip_country_code__value, '')::varchar(1024)                                       as ip_country_code,
       nullif(property_hubspot_user_token__value, '')::varchar(1024)                                    as hubspot_user_token,
       nullif(property_hubspot_owner_id__value, '')::bigint                                             as hubspot_owner_id,
       property_hs_v2_date_entered_salesqualifiedlead__value::timestamp without time zone               as hs_lifecyclestage_salesqualifiedlead_date,
       property_hs_v2_date_entered_marketingqualifiedlead__value::timestamp without time zone           as hs_lifecyclestage_marketingqualifiedlead_date,
       property_hs_v2_date_entered_lead__value::timestamp without time zone                             as hs_lifecyclestage_lead_date,
       nullif(property_hs_lead_status__value, '')::varchar(1024)                                        as hs_lead_status,
       nullif(property_hs_analytics_source_data_2__value, '')::varchar(1024)                            as hs_analytics_source_data_2,
       nullif(property_hs_analytics_source_data_1__value, '')::varchar(1024)                            as hs_analytics_source_data_1,
       nullif(property_hs_analytics_source__value, '')::varchar(1024)                                   as hs_analytics_source,
       property_hs_analytics_first_visit_timestamp__value::timestamp without time zone                  as hs_analytics_first_visit_timestamp,
       least(property_hs_analytics_first_visit_timestamp__value::timestamp,
             property_createdate__value)                                                                as earliest_timestamp,
       nullif(property_hs_analytics_first_url__value, '')::varchar(1024)                                as hs_analytics_first_url,
       nullif(property_firstname__value, '')::varchar(1024)                                             as firstname,
       nullif(property_emailtype__value, '')::varchar(1024)                                             as email_type,
       nullif(property_email__value, '')::varchar(1024)                                                 as email,
       date_trunc('day', property_hubspot_owner_assigneddate__value)                                    as hubspot_owner_assigned_date, --todo-migration: check this column
       property_createdate__value::timestamp without time zone                                          as createdate,
       vid::bigint                                                                                      as contact_id,
       nullif(property_bdr_campaign__value, '')::varchar(2048)                                          as bdr_campaign,
       case
           when property_strategic__value = 'true' then true
           when property_strategic__value = 'false' then false
           when property_strategic__value = '' then null end ::boolean                                  as strategic,
       property_notes_last_contacted__value::timestamp                                                  as notes_last_contacted,
       --         nullif(property_bdr_qualification__value, '')::varchar(1024) as bdr_qualification,
       nullif(property_bdr_assigned__value, '')::bigint                                                 as bdr_assigned,
       property_associatedcompanyid__value__double::bigint::bigint                                      as associatedcompanyid,
       nullif(property_zip__value, '')::varchar(64)                                                     as zip,
       nullif(property_industry__value, '')::varchar(256)                                               as industry,
       nullif(property_phone__value, '')::varchar(64)                                                   as phone,
       nullif(property_anonymous_order_uuid__value, '')::varchar(512)                                   as first_cart_uuid,
       rank() over (partition by first_cart_uuid order by createdate, contact_id)                       as rnk_asc_cart,     
       false                                                                                            as is_legacy,
       nullif(property_sf_18_digit_id__value, '')::varchar(1024)                                        as sf_18_digit_id

from {{ source('ext_hubspot', 'contacts') }}
left join {{ ref ('hubspot_spam_contacts') }} hsc on hsc.recipient = property_email__value and property_createdate__value::timestamp >='2023-01-01'
where hsc.recipient is null or property_lifecyclestage__value = 'customer' --filtering out spam