-- Freshdesk Tags

-- A Frehsdesk ticket can have multiple tags (~3 max)
-- in this model we explode tags into various rows per ID
-- This has a many to one relationship with tickets, used directly in Looker

with tickets as (
    select id,
           replace(replace(tags,'[',''), ']', '') as ctags
    from {{ ref('freshdesk_tickets') }} as ft
    where ft.tags is not null 
        and ft.tags <> '[]' -- Empty tags are not null but have this symbol
        and ft._is_latest -- Necessary filter to avoid duplicates
), sequence as (
  select 1 as n union all
  select 2 union all
  select 3
)
select
       tickets.id as ticket_id,
       TRIM(SPLIT_PART(tickets.ctags, ',', seq.n)) as ticket_tag
from sequence as seq
inner join tickets ON seq.n <= REGEXP_COUNT(tickets.ctags, ',') + 1