{{ config(materialized='table') }}

-- The source Type codes. Only h, u and t occur in this extract; the rest are carried so the
-- dimension stays complete if the source widens.

select
    type_code,
    type_description
from {{ ref('seed_property_type') }}
