{{ config(materialized='table') }}

-- What vendors hoped for against what the market paid.
--
-- NOT INCREMENTAL, deliberately. This is an aggregate: its size is bounded by dimension
-- cardinality (suburbs x months, agents x region-months), not by event volume, so it stays
-- small while the fact grows. Making it incremental was tried and reverted — an aggregate
-- that denormalises a dimension attribute goes stale when that attribute changes, and
-- extending the date spine silently invalidates every series. Both were caught by
-- tests/equivalence_harness.py. Paying that complexity to rebuild ~13k rows is a bad trade.
--
-- A property is passed in or vendor-bid at a known figure, then later sells at a known price.
-- Computable only because the figure on an unsold outcome is preserved as bid_amount rather
-- than averaged in with sale prices under a single `price` column (ADR-0005).

with bids as (

    select
        property_key,
        suburb_key,
        event_date as bid_date,
        bid_amount
    from {{ ref('fact__listing_outcome') }}
    where not is_sold
      and bid_amount is not null
      ),

sales as (

    select
        property_key,
        event_date as sale_date,
        sale_price
    from {{ ref('fact__listing_outcome') }}
    where is_sold
      and sale_price is not null
      ),

-- The first sale AFTER each bid. Direction matters: a sale that preceded the bid says nothing
-- about whether that vendor's expectation was met.
paired as (

    select
        bids.property_key,
        bids.suburb_key,
        bids.bid_date,
        bids.bid_amount,
        min(sales.sale_date) as eventual_sale_date
    from bids
    inner join sales
        on bids.property_key = sales.property_key
       and sales.sale_date > bids.bid_date
    group by 1, 2, 3, 4

)

select
    paired.property_key,
    property.street_address,
    suburb.suburb_name,
    suburb.region_name,
    paired.bid_date,
    paired.bid_amount,
    paired.eventual_sale_date,
    sales.sale_price                                              as eventual_sale_price,
    sales.sale_price - paired.bid_amount                          as gap_amount,
    sales.sale_price / nullif(paired.bid_amount, 0) - 1           as gap_pct,
    date_diff('day', paired.bid_date, paired.eventual_sale_date)  as days_to_sale
from paired
inner join sales
    on paired.property_key = sales.property_key
   and paired.eventual_sale_date = sales.sale_date
inner join {{ ref('dim_property') }} as property on paired.property_key = property.property_key
inner join {{ ref('dim_suburb') }}   as suburb   on paired.suburb_key = suburb.suburb_key
