with hs_deals as (select
    createdate,
    hs_latest_associated_company_id as hs_company_id,
    hs_latest_associated_contact_id as hs_contact_id,
    nullif(Trim(deal_id), '')::bigint as deal_id,
    -- See also H3D-11269
    case
        when hs_contact_id = hs_company_id then 'property_unknown'
    end as property_eval
    from {{ source('data_lake', 'hubspot_deals') }}
    inner join {{ ref('hubspot_dealstages') }}
    on
             hubspot_deals.dealstage = hubspot_dealstages.dealstage_internal_label
    where true
                        -- Filter for supply pipeline deals only, we get Drupal deals from below or statement or the extract
         and (dealstage in (select dealstage_internal_label
                 from {{ ref('hubspot_dealstages') }}
                 where pipeline = 'supply'))
         and hubspot_deals.pipeline in (
             'f3990922-a115-4de1-bb13-073a89b13164', 'default'
         )
                        -- Filter to include where we could match on the deal_number to deal_id
                        --This we need to do so that we only take into account valid Drupal deals from Hubspot
                        --There are a bunch of incorrect or accidentally created deals etc there
         or exists(select 1
             from {{ source('data_lake', 'adhoc_drupal_closed_sales_20191211') }} as data_lake_adhoc_drupal_closed_sales_20191211
             where data_lake.hubspot_deals.deal_number = data_lake_adhoc_drupal_closed_sales_20191211.order_id::text)),

--Need to add this as there are many deals unmatched if we don't take into account contacts matched on email here
drupal_deals as (
    select
        data_lake_adhoc_drupal_closed_sales_20191211.closed_date as createdate,
        -- Checked and there is no overlap on order_id and hubspot_deal_id
        data_lake_adhoc_drupal_closed_sales_20191211.order_id as deal_id,
        -- Need to get these from dim_clients (dependency)
        hs_contacts.hs_contact_id as hs_contact_id,
        hs_contacts.hs_associated_company_id as hs_company_id,
        case
             when hs_contact_id = hs_company_id then 'property_unknown'
        end as property_eval
    from {{ source('data_lake', 'adhoc_drupal_closed_sales_20191211') }} as data_lake_adhoc_drupal_closed_sales_20191211

    left join (select
                 email,
                 max(contact_id) as hs_contact_id,
                 max(associatedcompanyid) as hs_associated_company_id
             from {{ source('data_lake', 'hubspot_contacts') }}
             group by 1) as hs_contacts on hs_contacts.email = data_lake_adhoc_drupal_closed_sales_20191211.customer_email

    --These are already included above
    where not exists(select 1
                             from {{ source('data_lake', 'hubspot_deals') }} data_lake_hubspot_deals
             where
        data_lake_hubspot_deals.deal_number = data_lake_adhoc_drupal_closed_sales_20191211.order_id
        and data_lake_hubspot_deals.pipeline in (
             'f3990922-a115-4de1-bb13-073a89b13164', 'default'
         )
 ))

select
    createdate,
    deal_id,
    hs_contact_id,
    hs_company_id,
    property_eval
from hs_deals
union all
select
    createdate,
    deal_id,
    hs_contact_id,
    hs_company_id,
    property_eval
from drupal_deals
