    with suppliers as (
         select s.id                                                                         as supplier_id,
                s.name                                                                       as supplier_name,
                s.created                                                                    as created_at,
                s.address_id,
                s.is_accepting_auctions                                                      as is_able_to_accept_auctions,
                s.allow_for_rfq                                                              as is_eligible_for_rfq,
                s.is_eligible_for_vqc,
                s.currency_code,
                s.unit_preference,
                s.monthly_order_value_target,                
                s.is_suspended,
                
                ssu.created                                                                  as supplier_user_created_at,
                ssu.email                                                                    as supplier_email,
                ssu.email_domain                                                             as supplier_email_domain,
                ssu.email <> null                                                            as has_email, --todo-migration-test = from is
                ssu.is_internal                                                              as is_test_supplier,
                ssu.is_protolabs                                                             as is_protolabs,
                ssu.is_anonymized                                                            as is_anonymized,
                ssu.last_active_at                                                           as last_sign_in_at,
                datediff('day', last_sign_in_at, current_date)                               as last_sign_in_at_days_ago

         from {{ ref('suppliers') }} s
             left outer join {{ ref('supplier_users') }} as ssu on s.id = ssu.supplier_id
         ), 
     unique_suppliers as (select *, row_number() over (partition by supplier_id order by supplier_user_created_at asc nulls last) as rn from suppliers)
select supplier_id,
       created_at,
       address_id,
       supplier_name,
       supplier_email,
       supplier_email_domain,
       has_email,
       is_suspended,
       is_test_supplier,
       is_protolabs,
       is_anonymized,
       is_able_to_accept_auctions,
       is_eligible_for_rfq,
       is_eligible_for_vqc,
       currency_code,
       unit_preference,
       last_sign_in_at,
       last_sign_in_at_days_ago,
       monthly_order_value_target
from unique_suppliers
where rn = 1
