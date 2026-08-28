{{ config(materialized='table') }}

-- One row per suburb. Postcode, region, council, CBD distance and property count are
-- attributes of the suburb, not of a sale: verified across all 377 with zero violations.
-- They are taken with min() purely for determinism — every row within a suburb agrees.

with canonical as (

    {{ canonical_name(ref('int__listing_outcome_deduped'), 'suburb_key', 'suburb_name') }}

),

attributes as (

    select
        suburb_key,
        min(postcode)        as postcode,
        min(region_name)     as region_name,
        min(council_area)    as council_area,
        min(cbd_distance_km) as cbd_distance_km,
        min(property_count)  as property_count
    from {{ ref('int__listing_outcome_deduped') }}
    group by suburb_key

)

select
    canonical.suburb_key,
    canonical.suburb_name,
    attributes.postcode,
    attributes.region_name,
    attributes.council_area,
    attributes.cbd_distance_km,
    attributes.property_count
from canonical
inner join attributes using (suburb_key)
