{{ config(materialized='table') }}

-- One row per dwelling: identity only.
--
-- Keyed on suburb AND address because address alone is not unique — "14 Moray St" occurs in
-- seven suburbs, and merging them would splice unrelated sale histories together.
--
-- No rooms or type here: both differ between outcomes for the same dwelling (112 and 72
-- properties respectively), so they belong on the fact as at-the-outcome attributes.

with canonical as (

    {{ canonical_name(
        ref('int__listing_outcome_deduped'),
        'suburb_key, street_address_key',
        'street_address'
    ) }}

)

select
    {{ dbt_utils.generate_surrogate_key(['suburb_key', 'street_address_key']) }} as property_key,
    suburb_key,
    street_address_key,
    street_address
from canonical
