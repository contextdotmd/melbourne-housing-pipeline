{{ config(
    materialized = 'incremental',
    incremental_strategy = 'append',
    on_schema_change = 'fail',
) }}

-- Landing rows in domain vocabulary, values normalised, cardinality untouched.
--
-- Append-only: this is the warehouse's copy of the immutable event log, and it accumulates
-- every load ever ingested. Nothing here is ever updated or deleted, which is what lets the
-- deduplication layer downstream see a property's complete history when it reprocesses.
-- Correcting a record means a NEW row arriving in a later load, not this one changing.
--
-- Keys are lowercased and display forms kept alongside them, because the source spells the
-- same suburb and the same agency more than one way (Croydon/croydon, VICPROP/VICProp).
-- Keying on the raw string would split one entity into several dimension members.
--
-- Nothing here derives meaning or drops a row: `is_sold`, the price split and the recapture
-- collapse all belong to the intermediate layer.

select
    -- geography
    lower(trim(suburb))                          as suburb_key,
    trim(suburb)                                 as suburb_name,
    trim(postcode)                               as postcode,
    trim(regionname)                             as region_name,
    trim(councilarea)                            as council_area,
    distance                                     as cbd_distance_km,
    propertycount                                as property_count,

    -- property identity (address alone is not unique: "14 Moray St" is in seven suburbs)
    trim(address)                                as street_address,
    lower(trim(address))                         as street_address_key,

    -- as-at-this-outcome attributes, not facts about the dwelling (see docs/data-model.md)
    rooms                                        as rooms,
    lower(trim("type"))                          as type_code,

    -- outcome
    upper(trim(method))                          as method_code,
    "date"                                       as event_date,

    -- agent
    lower(trim(sellerg))                         as agent_key,
    trim(sellerg)                                as agent_name,

    -- measure, still overloaded at this layer: split happens in int__listing_outcome
    price                                        as reported_price,

    -- lineage back to the ingest receipt
    landing._load_id                            as load_id,
    landing._source_row                         as source_row,

    -- When this row was ingested, as distinct from when the event happened. Restatements
    -- arrive long after their event date, so ordering loads needs ingestion time.
    receipt.started_at                           as load_started_at

from {{ source('landing', 'listing_outcome') }} as landing
left join {{ source('landing', 'ingest_receipt') }} as receipt
    on landing._load_id = receipt.load_id

{% if is_incremental() %}
-- Loads already staged are skipped entirely: appending them twice would double the log and
-- break the row ledger. This is what makes a re-run of the DAG harmless.
where landing._load_id not in (select distinct load_id from {{ this }})
{% endif %}
