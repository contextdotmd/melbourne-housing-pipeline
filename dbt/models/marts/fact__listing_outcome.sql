{#- Partition overwrite: the unit of work is the property, which is a partition and
    not a unique key. This model's real unique key is asserted by a test rather than
    claimed by config. See ADR-0011. -#}
{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'sync_all_columns',
    pre_hook = "{{ overwrite_affected_partitions('property_key') }}",
) }}

{#- dbt cannot infer a ref() that only appears inside a conditional, so the scope
    macro's dependency on staging is declared explicitly. -#}
-- depends_on: {{ ref('stg__listing_outcome') }}

-- One row per genuine listing outcome (ADR-0004).
--
-- Dimension keys are recomputed from the same column expressions the dimensions use, rather
-- than joined out of them: the surrogate keys are deterministic MD5 hashes, so recomputing is
-- both cheaper and impossible to get half-right. The relationships tests prove every key
-- resolves.
--
-- suburb_key is carried alongside property_key so suburb analysis need not route through the
-- 58,696-row property dimension. assert_fact_suburb_agrees_with_property.sql keeps the two
-- from drifting.

select
    -- The surviving row of a cluster is unique on (signature, event_date): each cluster has
    -- exactly one earliest date.
    {{ dbt_utils.generate_surrogate_key(['business_signature', 'event_date']) }}
        as listing_outcome_key,

    -- foreign keys
    {{ dbt_utils.generate_surrogate_key(['suburb_key', 'street_address_key']) }}
        as property_key,
    suburb_key,
    agent_key,
    event_date,
    method_code,
    type_code,

    -- as-at-this-outcome attribute, not a fact about the dwelling
    rooms,

    -- mutually exclusive measures (ADR-0005)
    sale_price,
    bid_amount,
    sale_price_is_disclosed,
    is_sold,
    is_auction,

    -- lineage back to the ingest receipt
    load_id,
    source_row

from {{ ref('int__listing_outcome_deduped') }}

{% if is_scoped_run() %}
-- Same unit of work as the layer above: whole properties in, whole properties out. Keyed on
-- the property rather than listing_outcome_key so that an outcome which has just become a
-- restatement is actually removed, instead of lingering because nothing replaced it.
where (suburb_key, street_address_key) in (
    select suburb_key, street_address_key from ({{ affected_properties() }}) as scope
)
{% endif %}
