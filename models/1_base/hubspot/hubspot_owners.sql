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
   from {{ source('ext_gsheets_v2', 'hr_bamboo_employees') }}
),
sales_hubspot_ids as(
    select
    hubspot_id,
    firstname,
    lastname,
    fullname,
    email
    from {{ ref('seed_hs_id_sales_team') }} 
)
select
	   owner_id,
       ho.email as email,
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
	from {{ ref('hubspot_owners_base') }} ho 
	left join employees hr on (ho.first_name = hr.first_name and ho.last_name = hr.last_name) or hr.email = ho.email
--     WHERE (ho.first_name, ho.last_name) NOT IN (
--     SELECT firstname, lastname
--     FROM sales_hubspot_ids
-- )
  

 union all
select 
       hubspot_id as owner_id,
       hsid.email as email,
       firstname as first_name,
       lastname as last_name,
       fullname as name,
       null as user_id,
       null as created_at,
       null as updated_at,
       null as archived,
       null as loaded_at,
       null as primary_team_name,
       null as is_current,
	   null as location,
      null as office_location
    from sales_hubspot_ids hsid
    WHERE (hsid.hubspot_id) NOT IN (
    SELECT owner_id
    FROM {{ ref('hubspot_owners_base') }}
    )
    order by 1
    
--todo-migration-research depends on hubspot_owners_base that needs the semi-structured data processing fixed
