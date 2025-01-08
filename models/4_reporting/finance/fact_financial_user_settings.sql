with netsuite_agg as (
select enc.parent__internalid,
       case when enc.parent__internalid is not null then count(*) else null end as child_count
from {{ source('ext_netsuite', 'customer') }} as enc
group by 1)

select
       ufs.user_id as platform_customer_id,
       enc.internalid as netsuite_company_internal_id,
       enc.companyname as company_name,
       case
           when enc.parent__internalid is null then 'Parent'
           else 'Child'
       end as netsuite_company_type,
       nagg.child_count,
       enc.email,
       enc.custentity_financial_contact_email as financial_contact_email,
       enc.custentity_manual_financial_contact as manual_financial_contact_email,
       enc.custentity_nov_dun_exclude_from_dunning as dunning_paused,
       enc.custentity_dunning_pause_date as dunning_pause_at,
       enc.custentity7 as paused_by_dunning_manager,
       enc.custentity8 as dunning_manager,
       enc.custentity5 as days_excluded_from_dunning,
       enc.custentity_vat_number_required as is_vat_required,
       enc.custentity_vat_number as vat_number,
       enc.parent__internalid as netsuite_parent_internal_id,
       enc.parent__name as parent_company_name,
       enc.subsidiary__name as subsidiary,
       enc.terms__name as netsuite_terms,
       ufs.tax_number as platform_terms,
       ufs.credit_amount_limit as platform_credit_limit,
       ufs.downpayment_order_limit as platform_downpayment_limit,
       ufs.net_days as platform_net_days,
       ufs.is_pay_later_allowed,
       ufs.team_id,
       ufs.financial_contact_mail,
       ufs.financial_contact_first_name,
       ufs.financial_contact_last_name
from {{ source('ext_netsuite', 'customer') }} as enc
left join {{ ref('network_services', 'gold_users_financial_settings') }} as ufs on enc.internalid = ufs.netsuite_customer_id
left join netsuite_agg as nagg on enc.internalid = nagg.parent__internalid