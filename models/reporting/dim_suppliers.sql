with stg_states as (
         with us_states as (
             select distinct lower(replace(sa.administrative_area, 'US-', '')) as administrative_area,
                             sa.address_id                                     as address_id,
                             states.state                                      as state
             from {{ ref('addresses') }} sa
                      left join {{ ref('states') }} as states
                                on lower(states.code) = lower(replace(sa.administrative_area, 'US-', ''))
             where sa.country_id = '237'
         )
         select address_id,
                coalesce(states.state, us_states.state) as state
         from us_states
                  left join {{ ref('states') }} states
                            on lower(states.state) = lower(replace(us_states.administrative_area, 'US-', ''))
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
            s.is_suspended,
            s.is_accepting_auctions                                                      as is_active,
            s.currency_code,
            s.unit_preference,
            sa.country_id,
            c.name                                                                       as country_name,
            c.alpha2_code                                                                as country_code,
            c.continent,
            sa.locality                                                                  as city,
            states.state
        from {{ ref('suppliers') }} s
                left outer join {{ ref('addresses') }} sa on sa.address_id = s.address_id
                left outer join {{ ref('countries') }} c on c.country_id = sa.country_id
                left outer join {{ ref('supplier_users') }} as ssu on s.id = ssu.supplier_id
                left outer join {{ ref('users') }} as su on ssu.user_id = su.user_id
                left join stg_states as states on states.address_id = s.address_id
        where not su.is_internal),
        t2 as (select *, row_number() over (partition by supplier_id order by create_date desc nulls last) as rn from t1)
    select supplier_id,
        create_date,
        address_id,
        supplier_name,
        full_name,
        supplier_email,
        is_suspended,
        is_active,
        currency_code,
        unit_preference,
        country_id,
        country_name,
        country_code,
        continent,
        city,
        state
    from t2
    where rn = 1