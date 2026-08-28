{{ config(materialized='view') }}

-- Properties that sold twice with a disclosed price: holding period and annualised return.
--
-- Only trustworthy because the re-scrape recaptures were collapsed upstream. Left in, the
-- republished December snapshots would surface here as thousands of "resales" seven days
-- apart at exactly zero percent change.

with disclosed_sales as (

    select
        property_key,
        suburb_key,
        event_date,
        sale_price
    from {{ ref('fact__listing_outcome') }}
    where is_sold
      and sale_price is not null

),

sequenced as (

    select
        *,
        lag(event_date) over w as previous_sale_date,
        lag(sale_price) over w as previous_sale_price
    from disclosed_sales
    window w as (partition by property_key order by event_date, sale_price)

)

select
    sequenced.property_key,
    property.street_address,
    suburb.suburb_name,
    suburb.region_name,
    sequenced.previous_sale_date,
    sequenced.previous_sale_price,
    sequenced.event_date                                                as sale_date,
    sequenced.sale_price,
    date_diff('day', sequenced.previous_sale_date, sequenced.event_date) as days_held,
    sequenced.sale_price - sequenced.previous_sale_price                 as gain_amount,
    sequenced.sale_price / nullif(sequenced.previous_sale_price, 0) - 1  as gain_pct,
    -- annualised: (1 + total return) ^ (365 / days held) - 1
    power(
        sequenced.sale_price / nullif(sequenced.previous_sale_price, 0),
        365.0 / nullif(date_diff('day', sequenced.previous_sale_date, sequenced.event_date), 0)
    ) - 1                                                                as annualised_return
from sequenced
inner join {{ ref('dim_property') }} as property on sequenced.property_key = property.property_key
inner join {{ ref('dim_suburb') }}   as suburb   on sequenced.suburb_key = suburb.suburb_key
where sequenced.previous_sale_date is not null
  -- A pair closer together than the recapture window is a republication signature, not a
  -- resale. The upstream collapse should mean none reach here; this is a regression guard.
  and date_diff('day', sequenced.previous_sale_date, sequenced.event_date)
      > {{ var('recapture_window_days') }}
