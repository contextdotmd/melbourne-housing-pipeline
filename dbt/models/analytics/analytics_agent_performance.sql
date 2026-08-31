{{ config(materialized='table') }}

-- Agent x region x month.
--
-- NOT INCREMENTAL, deliberately. This is an aggregate: its size is bounded by dimension
-- cardinality (suburbs x months, agents x region-months), not by event volume, so it stays
-- small while the fact grows. Making it incremental was tried and reverted — an aggregate
-- that denormalises a dimension attribute goes stale when that attribute changes, and
-- extending the date spine silently invalidates every series. Both were caught by
-- tests/equivalence_harness.py. Paying that complexity to rebuild ~13k rows is a bad trade.
--
-- Auction share is derived from dim_sale_method.is_auction, carried onto the fact. Deriving
-- it instead from a single Method code risks a metric that is mathematically incapable of
-- being non-zero, because the codes most associated with auctions are also the ones whose
-- price is never disclosed and which a price filter therefore removes.
--
-- No calendar grid here: agents legitimately come and go, and every window in this model is
-- within a month rather than across months, so there is no adjacency to preserve.

with enriched as (

    select
        fact.agent_key,
        agent.agent_name,
        suburb.region_name,
        calendar.month_start,
        fact.is_sold,
        fact.is_auction,
        fact.sale_price
    from {{ ref('fact__listing_outcome') }} as fact
    inner join {{ ref('dim_agent') }}  as agent    on fact.agent_key = agent.agent_key
    inner join {{ ref('dim_suburb') }} as suburb   on fact.suburb_key = suburb.suburb_key
    inner join {{ ref('dim_date') }}   as calendar on fact.event_date = calendar.date_day
    ),

aggregated as (

    select
        agent_key,
        agent_name,
        region_name,
        month_start,
        count(*)                              as listings,
        count(*) filter (where is_sold)       as sales,
        count(*) filter (where is_auction)    as auction_listings,
        coalesce(sum(sale_price), 0)          as gmv,
        median(sale_price)                    as median_sale_price
    from enriched
    group by 1, 2, 3, 4

)

select
    *,
    sales::double / nullif(listings, 0)            as clearance_rate,
    auction_listings::double / nullif(listings, 0) as auction_share,
    gmv / nullif(sum(gmv) over (partition by month_start, region_name), 0)
                                                   as region_gmv_share,
    -- No tie-break column: equal GMV must mean equal rank, not an alphabetical ordering
    -- disguised as one.
    dense_rank() over (
        partition by month_start, region_name order by gmv desc
    )                                              as rank_by_gmv_in_region
from aggregated
