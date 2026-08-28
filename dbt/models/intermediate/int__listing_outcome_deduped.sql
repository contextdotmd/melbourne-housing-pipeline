{{ config(materialized='table') }}

-- One row per genuine listing outcome: neither a restatement of another row nor a
-- republication of one. Complementary to quarantine_recaptured_listing by construction.

select
    business_signature,
    suburb_key,
    suburb_name,
    postcode,
    region_name,
    council_area,
    cbd_distance_km,
    property_count,
    street_address,
    street_address_key,
    rooms,
    type_code,
    method_code,
    event_date,
    agent_key,
    agent_name,
    is_sold,
    is_auction,
    sale_price,
    bid_amount,
    sale_price_is_disclosed,
    load_id,
    source_row
from {{ ref('int__listing_outcome_clustered') }}
where dq_status = 'ok'
