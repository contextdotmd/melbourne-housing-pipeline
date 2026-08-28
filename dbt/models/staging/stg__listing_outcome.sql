{{ config(materialized='view') }}

-- Landing rows in domain vocabulary, values normalised, cardinality untouched.
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
    _load_id                                     as load_id,
    _source_row                                  as source_row

from {{ source('landing', 'listing_outcome') }}
