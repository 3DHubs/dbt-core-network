{{
    config(
        materialized='incremental',
        post_hook = "analyze {{ this }}"
    )
}}

with prep_recipient as (
select
    emailcampaigngroupid,
    emailcampaignid,
    coalesce(emailcampaigngroupid, emailcampaignid) as campaign_id,
    appid,
    recipient as unencrypted_recipient,
    appname as campaign_type,
    sentby__created as sent_at,
    min(case when type = 'OPEN' and filteredevent = false then devicetype end) as device_type,
    count(case when type = 'SENT' then 1 end) as p_sent,
    count(case when type = 'DROPPED' then 1 end) as dropped,
    count(case when type = 'DELIVERED' then 1 end) delivered,
    count(case when type = 'BOUNCE' then 1 end) as bounced,
    count(case when type = 'OPEN' and filteredevent = false then 1 end) as opened,
    count(case when type = 'CLICK' and filteredevent = false  then 1 end) as clicked,
    count(case when type = 'FORWARD' then 1 end) as forwarded,
    count(distinct case when type = 'SENT' then recipient end) as unique_sent,
    count(distinct case when type = 'OPEN' and filteredevent = false  then recipient end) as unique_opened,
    count(distinct case when type = 'CLICK' and filteredevent = false  then recipient end) as unique_clicked,
    count(distinct case when type = 'FORWARD' then recipient end) as unique_forwarded,
    max(_sdc_batched_at) as latest_upload


    from {{ source('ext_hubspot', 'email_events') }}
    -- where emailcampaigngroupid='204944054' -- for testing purposes
    --     or emailcampaigngroupid='209153861'
    --     or emailcampaignid='209153476'
    -- or emailcampaigngroupid='199531106'

             {% if is_incremental() %}

         where _sdc_batched_at
             > (select max (latest_upload) from {{ this }})

             {% endif %}
group by 1,2,3,4,5,6,7 -- order by total_bounced desc limit 500
 )
select
    campaign_id,
    coalesce(c.name, cm.name) as campaign_name,
    coalesce(c.subject, cm.subject) as subject,
    sent_at,
    campaign_type, 
    coalesce(device_type, 'UNKNOWN') as device_type,
    md5(unencrypted_recipient) as recipient,
    u.contact_id as hubspot_contact_id,
    case when p_sent > 0 then p_sent - dropped end as sent,
    case when sent > 0 then true else false end as is_sent,
    delivered,
    case when delivered > 0 then true else false end is_delivered,
    opened,
    case when opened > 0 then true else false end is_opened,
    unique_opened,
    clicked,
    case when clicked > 0 then true else false end is_clicked,
    unique_clicked,
    forwarded,
    unique_forwarded,
    latest_upload
    from prep_recipient p
    left join {{ source('ext_hubspot', 'campaigns') }} c on c.id = p.campaign_id 
    left join {{ source('ext_hubspot', 'campaigns') }} cm on cm.id = p.emailcampaignid -- in case no emailcampaigngroupid is matching campaigns.
    left join {{ ref('stg_hs_contacts_union_legacy') }} u on u.email = p.unencrypted_recipient and u.rnk_desc_email = 1
    where p_sent > 0