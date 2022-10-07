select 
    d.date,
    value + default_value as first_leg_buffer_value,
    supplier_country,
    crossdock_country

    from {{ ref('seed_logistics_buffer_settings') }} s
            inner join {{ source('data_lake', 'dim_dates') }} d
     on  s.start_date <= d.date AND coalesce(s.end_date, getdate()) > d.date
        order by date