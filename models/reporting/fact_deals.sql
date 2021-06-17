-- placeholder model

{{
    config(
        bind = False,
        materialized = 'view'
    )
}}
select * from reporting.cube_deals