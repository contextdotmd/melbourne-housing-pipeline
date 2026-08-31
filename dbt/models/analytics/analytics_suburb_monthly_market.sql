{{ config(materialized='table') }}

-- Suburb x month market health.
--
-- NOT INCREMENTAL, deliberately. This is an aggregate: its size is bounded by dimension
-- cardinality (suburbs x months, agents x region-months), not by event volume, so it stays
-- small while the fact grows. Making it incremental was tried and reverted — an aggregate
-- that denormalises a dimension attribute goes stale when that attribute changes, and
-- extending the date spine silently invalidates every series. Both were caught by
-- tests/equivalence_harness.py. Paying that complexity to rebuild ~13k rows is a bad trade.
--
-- Built over a complete grid of suburbs and calendar months, not just the months that saw
-- activity. Only 108 days in three years carry any, so aggregating observed months alone
-- would let LAG compare non-adjacent months while looking entirely correct. That is what
-- dim_date is for.
--
-- Price per room substitutes for price per square metre: MELBOURNE_HOUSE_PRICES_LESS.csv
-- carries no area column, so the brief's example metric is not computable from it.

with grid as (

    select
        suburb.suburb_key,
        suburb.suburb_name,
        suburb.region_name,
        suburb.postcode,
        suburb.cbd_distance_km,
        months.month_start
    from {{ ref('dim_suburb') }} as suburb
    cross join (select distinct month_start from {{ ref('dim_date') }}) as months
    ),

measured as (

    select
        fact.suburb_key,
        calendar.month_start,
        count(*)                                          as listings,
        count(*) filter (where fact.is_sold)              as sales,
        count(fact.sale_price)                            as disclosed_sales,
        median(fact.sale_price)                           as median_sale_price,
        avg(fact.sale_price)                              as avg_sale_price,
        avg(fact.sale_price / nullif(fact.rooms, 0))      as avg_sale_price_per_room
    from {{ ref('fact__listing_outcome') }} as fact
    inner join {{ ref('dim_date') }} as calendar
        on fact.event_date = calendar.date_day
    group by 1, 2

),

joined as (

    select
        grid.suburb_key,
        grid.suburb_name,
        grid.region_name,
        grid.postcode,
        grid.cbd_distance_km,
        grid.month_start,
        coalesce(measured.listings, 0)  as listings,
        coalesce(measured.sales, 0)     as sales,
        coalesce(measured.disclosed_sales, 0) as disclosed_sales,
        measured.median_sale_price,
        measured.avg_sale_price,
        measured.avg_sale_price_per_room,
        -- null, not zero, when the suburb had no listings: 0/0 is unknown, not "nothing sold"
        measured.sales::double / nullif(measured.listings, 0) as clearance_rate
    from grid
    left join measured
        on grid.suburb_key = measured.suburb_key
       and grid.month_start = measured.month_start

)

select
    *,
    lag(median_sale_price) over w                       as median_sale_price_prev_month,
    median_sale_price - lag(median_sale_price) over w   as median_sale_price_mom_change,
    avg(median_sale_price) over (
        partition by suburb_key order by month_start
        rows between 2 preceding and current row
    )                                                   as median_sale_price_3m_avg,
    lag(clearance_rate) over w                          as clearance_rate_prev_month,
    -- No tie-break column: with one, every rank is unique and equal volumes could never
    -- share a rank.
    dense_rank() over (
        partition by month_start, region_name order by listings desc
    )                                                   as rank_by_listings_in_region
from joined
window w as (partition by suburb_key order by month_start)
