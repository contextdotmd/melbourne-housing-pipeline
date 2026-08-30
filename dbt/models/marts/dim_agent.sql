{{ config(materialized='table') }}

-- One row per selling agency.
--
-- The display spelling is the most frequent variant across ALL of an agency's rows, so an
-- agency touched by this run is recomputed from its complete history — a single new row could
-- change which spelling wins.

with scoped as (

    select * from {{ ref('int__listing_outcome_deduped') }}

),

canonical as (

    {{ canonical_name('scoped', 'agent_key', 'agent_name') }}

)

select * from canonical
