{{
    config(
        materialized='table'
    )
}}

select
	   owner_id,
       ho.email,
       ho.first_name,
       ho.last_name,
       name,
       user_id,
       created_at,
       updated_at,
       archived,
       loaded_at,
       primary_team_name,
       is_current,
	   location,
       case when location <> 'Chicago' then 'Amsterdam' end as office_location 
	from {{ ref('hubspot_owners_federated') }} ho 
	left join {{ source('ext_gsheets', 'hr_bamboo_employees') }} hr on (ho.first_name = hr.first_name and ho.last_name = hr.last_name) or hr.email = ho.email