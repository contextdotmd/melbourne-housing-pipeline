{{ config(materialized='table') }}

-- Derive meaning from the source codes.
--
-- `Method` decides whether the property transacted; `is_sold` comes from seed_sale_method so
-- the rule is declared once, as reference data, rather than restated in every model that
-- needs it.
--
-- The source `Price` column means two different things depending on that answer: consideration
-- paid on a sale, or the highest/vendor bid on a property that did not sell. Splitting it into
-- two mutually exclusive columns makes the contaminated-average mistake structurally
-- impossible instead of merely documented (ADR-0005).

with staged as (

    select * from {{ ref('stg__listing_outcome') }}

),

method as (

    select
        method_code,
        is_sold,
        is_auction
    from {{ ref('seed_sale_method') }}

)

select
    staged.suburb_key,
    staged.suburb_name,
    staged.postcode,
    staged.region_name,
    staged.council_area,
    staged.cbd_distance_km,
    staged.property_count,

    staged.street_address,
    staged.street_address_key,

    staged.rooms,
    staged.type_code,

    staged.method_code,
    staged.event_date,

    staged.agent_key,
    staged.agent_name,

    method.is_sold,
    method.is_auction,

    -- Mutually exclusive by construction: a row can never populate both.
    case when method.is_sold then staged.reported_price end       as sale_price,
    case when not method.is_sold then staged.reported_price end   as bid_amount,

    -- Disclosure is a property of the row, not of the method code: 10.1% of plain S rows
    -- carry no price at all, so this cannot be pushed onto dim_sale_method.
    coalesce(method.is_sold and staged.reported_price is not null, false)
                                                                  as sale_price_is_disclosed,

    staged.load_id,
    staged.source_row

from staged
inner join method
    on staged.method_code = method.method_code
