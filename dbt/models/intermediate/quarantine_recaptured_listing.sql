{{ config(materialized='table') }}

-- Rows excluded as republished snapshots. Deleted from the fact but not from the warehouse:
-- the exclusion has to be provable, inspectable and challengeable (ADR-0006).
--
-- The filter is the exact complement of int__listing_outcome_deduped's, both derived from the
-- same clustering model, so no row can fall between the two.

select
    {{ dbt_utils.generate_surrogate_key(['business_signature', 'event_date', 'source_row']) }}
        as quarantine_key,
    'suspected_recapture'          as reason,
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
where recapture_rank > 1
