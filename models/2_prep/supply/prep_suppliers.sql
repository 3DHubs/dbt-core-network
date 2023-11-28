    with suppliers as (
         select s.id                                                                         as supplier_id,
                s.created                                                                    as create_date,
                s.address_id,
                s.name                                                                       as supplier_name,
                su.email                                                                     as supplier_email,
                split_part(su.email, '@', 2)                                                 as supplier_email_domain,
                case
                    when su.email is null OR trim(su.email) = '' then false else true
                end                                                                          as has_email,
                s.is_suspended                                                               as is_suspended,
                case
                    when su.email !~ '@(3d)?hubs.com' then false else true
                end                                                                          as is_test_supplier,
                case
                    when su.email LIKE '%@protolabs%' then true else false 
                end                                                                          as is_protolabs,
                case
                    when su.email !~ '.*anonymized*.' then false else true 
                end                                                                          as is_anonymized,
                s.is_accepting_auctions                                                      as is_able_to_accept_auctions,
                s.allow_for_rfq                                                              as is_eligible_for_rfq,
                s.is_eligible_for_vqc,
                s.currency_code,
                s.unit_preference,
                s.monthly_order_value_target
         from {{ ref('suppliers') }} s
             left outer join {{ source('int_service_supply', 'supplier_users') }} as ssu on s.id = ssu.supplier_id
             left outer join {{ ref('prep_users') }} as su on ssu.user_id = su.user_id
         ), 
     unique_suppliers as (select *, row_number() over (partition by supplier_id order by create_date desc nulls last) as rn from suppliers)
select supplier_id,
       create_date,
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
       monthly_order_value_target
from unique_suppliers
where rn = 1
