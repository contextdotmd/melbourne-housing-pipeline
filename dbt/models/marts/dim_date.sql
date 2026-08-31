{{ config(materialized='table') }}

-- A contiguous calendar spine across the observed range, NOT the distinct dates in the data.
--
-- Only 108 days carry activity across nearly three years. Built from observed dates, a quiet
-- month would simply not exist, and LAG in the month-on-month calculations would silently
-- compare non-adjacent months while looking entirely correct.
--
-- generate_series is DuckDB syntax; the porting notes in the README give the equivalent for
-- BigQuery and Snowflake.

with bounds as (

    select
        date_trunc('month', min(event_date))                                  as spine_start,
        date_trunc('month', max(event_date)) + interval 1 month - interval 1 day as spine_end
    from {{ ref('int__listing_outcome_deduped') }}

),

spine as (

    select unnest(generate_series(spine_start, spine_end, interval 1 day))::date as date_day
    from bounds

)

select
    date_day,
    extract(year from date_day)                as calendar_year,
    extract(month from date_day)               as calendar_month,
    date_trunc('month', date_day)::date        as month_start,
    strftime(date_day, '%Y-%m')                as year_month,
    extract(dow from date_day)                 as day_of_week,
    strftime(date_day, '%A')                   as day_name,
    extract(dow from date_day) = 6             as is_saturday
from spine
