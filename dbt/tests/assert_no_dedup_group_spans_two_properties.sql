-- The load-bearing assumption of the incremental design.
--
-- Reprocessing is scoped by property. That is only complete if no deduplication group can
-- span two properties — otherwise reprocessing property A in isolation would leave a group
-- half-recomputed and silently corrupt property B.
--
-- True by construction, since both partition keys contain the property. Asserted anyway,
-- because it is the assumption the entire incremental path rests on, and a future change to
-- either signature could break it without any other test noticing.

select
    business_signature,
    count(distinct suburb_key || '|' || street_address_key) as distinct_properties
from {{ ref('int__listing_outcome_clustered') }}
group by business_signature
having count(distinct suburb_key || '|' || street_address_key) > 1
