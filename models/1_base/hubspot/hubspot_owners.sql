{{
    config(
        materialized='table'
    )
}}
with employees as ( --multiple rows are inserted in source, distinction not clear yet when this will happen
    select
    distinct
    email,
    first_name,
    last_name,
    location
   from {{ source('ext_gsheets', 'hr_bamboo_employees') }}
)
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
	   coalesce(location,'Amsterdam') as location,
       case when location <> 'Chicago' then 'Amsterdam' else 'Chicago' end as office_location 
	from {{ ref('hubspot_owners_federated') }} ho 
	left join employees hr on (ho.first_name = hr.first_name and ho.last_name = hr.last_name) or hr.email = ho.email