/* Unfortunately Freshdesk does not report merge events. This category of events needs to be parsed from raw text, 
   hence this procedure.
   
   A few caveats: (a) historically customer success/support reps could (accidentally) merge 
   tickets with themselves. This should not occur anymore. (b) in some scenarios tickets get merged with each other 
   (A --> B and B --> A) in which it is unclear what should be the right interpretation. Hopefully this provides a 
   useful start.

   The `numbers` CTE creates row numbers, supporting up to 10 ticket merges. `merges` extracts the merge type from
   the body_text field. If the body_text ends with a series of digits, the ticket got closed and merged with
   another ticket -- this is marked as type I merge. In all other cases the ticket has incoming merges -- the
   ticket itself remains open; other ticket(s) got closed -- this is marked as a type II merge. Beware that this
   may change in the future depending on how Freshdesk handles merges. ticket_ids are extracted from the body_text
   for both merge types and yields a comma separated list of IDs. The final select statement splits the comma-
   separated list into rows. I.e. for each individual merge there now will be a record.
  _______________
< Merge with care >
  ===============
                    \
                     \
                       ^__^
                       (oo)\_______
                       (__)\       )\/\
                           ||----w |
                           ||     ||

*/

{{
    config(
        materialized='table'
    )
}}

with numbers as (
    {% for i in range(1,11) %}
    select {{ i }} as num {% if not loop.last %} union all {% endif %}
    {% endfor %}),
        merges as (
            select id,
                created_at,
                ticket_id,
                body_text,
                nvl2(nullif(regexp_substr(body_text, '\\d+.?$'), ''), 1, 2)                            as merge_type, -- Type 1: this ticket
                -- got merged; Type 2: other tickets got closed and merged into this one. Depends on body text --
                -- if it ends with 6 digits it typically means Type 1.
                regexp_replace(trim(regexp_replace(body_text, '[^[:digit:]]', '$1 ')), '\\s{2,}', ',') as
                                                                                                            ticket_ids,
                regexp_count(body_text, '\\d{6,}')                                                     as num_tickets_merged
            from {{ ref('freshdesk_ticket_conversations') }}
            where true
            and lower(body_text) rlike 'ticket.*merged into' --todo-migration-test: changed ~ operator for rlike, check data
            and _is_latest)
select created_at                                 as merged_at,
        id                                         as ticket_conversation_id,
        ticket_id,
        split_part(ticket_ids, ',', numbers.num)   as linked_ticket_id,
        merge_type                                 as change, -- May need decoding?
        decode(change, 1, true, false)             as is_merged,
        decode(change, 2, true, false)             as has_incoming_merge,
        ticket_id = linked_ticket_id and is_merged as is_merged_with_self,
        body_text                                  as raw_merge_event
from merges
            join numbers on numbers.num <= regexp_count(ticket_ids, ',\\s') + 1