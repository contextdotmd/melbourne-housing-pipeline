{{ config(materialized='table') }}

-- One row per selling agency.

{{ canonical_name(ref('int__listing_outcome_deduped'), 'agent_key', 'agent_name') }}
