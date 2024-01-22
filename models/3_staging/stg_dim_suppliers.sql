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
         select s.supplier_id,
                s.create_date,
                s.address_id,
                s.supplier_name,
                trim(case
                         when upper(sa.first_name) = sa.first_name or lower(sa.first_name) = sa.first_name
                             then initcap(sa.first_name)
                         else sa.first_name end)                                                first_name_clean,
                trim(case
                         when upper(sa.last_name) = sa.last_name or lower(sa.last_name) = sa.last_name
                             then initcap(sa.last_name)
                         else sa.last_name end)                                                 last_name_clean,
                coalesce(nullif(first_name_clean || ' ' || last_name_clean, ''), 'Customer') as full_name,
                s.supplier_email,
                s.supplier_email_domain,
                s.is_suspended,
                s.is_able_to_accept_auctions,
                s.is_eligible_for_rfq,
                s.is_eligible_for_vqc,
                s.currency_code,
                s.unit_preference,
                s.last_sign_in_at,
                s.last_sign_in_at_days_ago,
                s.monthly_order_value_target,
                sa.country_id,
                c.name                                                                       as country_name,
                lower(c.alpha2_code)                                                         as country_code,
                lower(c.continent)                                                           as continent,
                sa.locality                                                                  as city,
                sa.postal_code,
                sa.address_line1 || ' ' || coalesce(sa.address_line2, ' ')                   as address,
                sa.lon                                                                       as longitude,
                sa.lat                                                                       as latitude,
                sa.timezone                                                                  as timezone,
                case when country_name = 'United States' then 'US'
                     when country_name = 'Mexico' then 'Mexico'
                     when country_name = 'India' THEN 'India'
                     when country_name = 'China' THEN 'China'
                     when country_name = 'United Kingdom' THEN 'United Kingdom'
                     when is_in_european_union THEN 'Europe'
                     else 'RoW' end                                                         as region,
                states.state
         from {{ ref('prep_suppliers') }} s
                left outer join {{ ref('addresses') }} sa
         on sa.address_id = s.address_id
             left outer join {{ ref('prep_countries') }} c on c.country_id = sa.country_id
             left join stg_states as states on states.address_id = s.address_id
         where (not is_test_supplier)
         or s.supplier_id=494 --JG 300622 requested by Arnoldas to include internal supplier id
         or s.supplier_id=19 -- requested by Matt to include internal account Shak IM RFQ
         ), 
     t2 as (select *, row_number() over (partition by supplier_id order by create_date desc nulls last) as rn from t1)
select supplier_id,
       create_date,
       address_id,
       supplier_name,
       full_name,
       supplier_email,
       supplier_email_domain,
       is_suspended,
       is_able_to_accept_auctions,
       is_eligible_for_rfq,
       is_eligible_for_vqc,
       currency_code,
       unit_preference,
       last_sign_in_at,
       last_sign_in_at_days_ago,
       monthly_order_value_target,
       country_id,
       country_name,
       country_code,
       continent,
       region,
       city,
       postal_code,
       address,
       longitude,
       latitude,
       state,
       timezone
from t2
where rn = 1
