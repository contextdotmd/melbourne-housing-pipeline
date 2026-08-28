-- The fact carries suburb_key as well as property_key, so suburb queries need not route
-- through the 58,696-row property dimension. That denormalisation is only safe while the two
-- agree, so assert it rather than assume it.

select
    fact.listing_outcome_key,
    fact.suburb_key   as fact_suburb_key,
    prop.suburb_key   as property_suburb_key
from {{ ref('fact__listing_outcome') }} as fact
inner join {{ ref('dim_property') }} as prop
    on fact.property_key = prop.property_key
where fact.suburb_key != prop.suburb_key
