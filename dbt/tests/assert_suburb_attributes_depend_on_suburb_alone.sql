-- Postcode, region, council, CBD distance and property count are modelled as attributes of
-- the suburb. That holds in this extract with zero violations, but it is an assumption about
-- the source, not a guarantee — so it is asserted on every build rather than trusted.

select
    suburb_key,
    count(distinct postcode)        as postcodes,
    count(distinct region_name)     as regions,
    count(distinct council_area)    as councils,
    count(distinct cbd_distance_km) as distances,
    count(distinct property_count)  as property_counts
from {{ ref('stg__listing_outcome') }}
group by suburb_key
having count(distinct postcode) > 1
    or count(distinct region_name) > 1
    or count(distinct council_area) > 1
    or count(distinct cbd_distance_km) > 1
    or count(distinct property_count) > 1
