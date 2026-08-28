{{ config(materialized='table') }}

-- The source Method codes. `is_sold` is the authoritative definition of a transaction and is
-- declared once here as reference data (ADR-0004).
--
-- Whether a price was *disclosed* is deliberately absent: it is not a function of the code.
-- 10.1% of plain S rows carry no price, so disclosure is recorded per row on the fact.

select
    method_code,
    method_description,
    is_sold,
    is_auction
from {{ ref('seed_sale_method') }}
