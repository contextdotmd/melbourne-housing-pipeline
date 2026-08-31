{{ config(materialized='view') }}

-- Every row excluded from the fact, with the reason it was excluded and the event date it was
-- collapsed into. Deleted from the star but not from the warehouse: an exclusion has to be
-- provable, inspectable and challengeable (ADR-0006).
--
-- Two reasons, two different phenomena:
--   suspected_restatement  a later correction of the same event, usually a price published
--                          after the fact. Same date, different content.
--   suspected_recapture    a republished snapshot of an earlier event. Same content,
--                          different date.

select
    -- Row identity, not content identity: repeated snapshot loads can quarantine the same
    -- (signature, date, source_row) once per load, so only the physical row is unique.
    {{ dbt_utils.generate_surrogate_key(['load_id', 'source_row']) }}
        as quarantine_key,
    dq_status                      as reason,
    business_signature,
    surviving_event_date,
    days_since_previous_appearance,
    event_date,
    suburb_key,
    suburb_name,
    street_address,
    rooms,
    type_code,
    method_code,
    agent_key,
    agent_name,
    is_sold,
    sale_price,
    bid_amount,
    load_id,
    source_row
from {{ ref('int__listing_outcome_clustered') }}
where dq_status != 'ok'
