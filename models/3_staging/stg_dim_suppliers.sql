with stg_states as (
    with us_states as (
        select distinct lower(replace(sa.administrative_area, 'US-', '')) as administrative_area,
                        sa.address_id                                     as address_id,
                        states.state                                      as state
        from {{ ref('addresses') }} sa
                      left join {{ ref('seed_states') }} as states
        on lower(states.code) = lower(replace (sa.administrative_area, 'US-', ''))
        where sa.country_id = '237'
    )
    select address_id,
           coalesce(states.state, us_states.state) as state
    from us_states
             left join {{ ref('seed_states') }} states
    on lower(states.state) = lower(replace (us_states.administrative_area, 'US-', ''))
),
     t1 as (
         select s.id                                                                         as supplier_id,
                s.created                                                                    as create_date,
                s.address_id,
                s.name                                                                       as supplier_name,
                trim(case
                         when upper(sa.first_name) = sa.first_name or lower(sa.first_name) = sa.first_name
                             then initcap(sa.first_name)
                         else sa.first_name end)                                                first_name_clean,
                trim(case
                         when upper(sa.last_name) = sa.last_name or lower(sa.last_name) = sa.last_name
                             then initcap(sa.last_name)
                         else sa.last_name end)                                                 last_name_clean,
                coalesce(nullif(first_name_clean || ' ' || last_name_clean, ''), 'Customer') as full_name,
                su.mail                                                                      as supplier_email,
                s.is_suspended                                                               as is_suspended,
                s.is_accepting_auctions                                                      as is_able_to_accept_auctions,
                s.allow_for_rfq                                                              as is_eligible_for_rfq,
                s.currency_code,
                s.unit_preference,
                s.monthly_order_value_target,
                sa.country_id,
                c.name                                                                       as country_name,
                lower(c.alpha2_code)                                                         as country_code,
                lower(c.continent)                                                           as continent,
                sa.locality                                                                  as city,
                case when country_name = 'United States' then 'US'
                     when country_name = 'Mexico' then 'Mexico'
                     when country_name = 'India' THEN 'India'
                     when country_name = 'China' THEN 'China'
                     when lower(continent) = 'eu' THEN 'Europe'
                     else 'RoW' end                                                         as region,
                states.state
         from {{ ref('suppliers') }} s
                left outer join {{ ref('addresses') }} sa
         on sa.address_id = s.address_id
             left outer join {{ ref('prep_countries') }} c on c.country_id = sa.country_id
             left outer join {{ source('int_service_supply', 'supplier_users') }} as ssu on s.id = ssu.supplier_id
             left outer join {{ ref('users') }} as su on ssu.user_id = su.user_id
             left join stg_states as states on states.address_id = s.address_id
         where supplier_email !~ '@(3d)?hubs.com'),
     t2 as (select *, row_number() over (partition by supplier_id order by create_date desc nulls last) as rn from t1)
select supplier_id,
       create_date,
       address_id,
       supplier_name,
       full_name,
       supplier_email,
       is_suspended,
       is_able_to_accept_auctions,
       is_eligible_for_rfq,
       currency_code,
       unit_preference,
       monthly_order_value_target,
       country_id,
       country_name,
       country_code,
       continent,
       region,
       city,
       state
from t2
where rn = 1