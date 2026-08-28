{{ config(materialized='view') }}

-- Group republished snapshots of the same event, so the survivor and the quarantine can be
-- derived from one expression instead of two.
--
-- The signature covers every business column EXCEPT the date. That is the whole point: the
-- source republished a snapshot weekly over the Christmas shutdown, stamping each copy with
-- the capture date, so any key that includes the date is structurally blind to the artefact.
--
-- A cluster breaks when consecutive appearances are more than `recapture_window_days` apart,
-- which preserves genuine relistings months later (ADR-0006).

{% set window_days = var('recapture_window_days') %}

with ordered as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'suburb_key',
            'street_address_key',
            'rooms',
            'type_code',
            'method_code',
            'agent_key',
            'sale_price',
            'bid_amount',
        ]) }} as business_signature
    from {{ ref('int__listing_outcome') }}

),

gapped as (

    select
        *,
        date_diff(
            'day',
            lag(event_date) over (
                partition by business_signature
                order by event_date, source_row
            ),
            event_date
        ) as days_since_previous_appearance
    from ordered

),

clustered as (

    select
        *,
        sum(
            case
                when days_since_previous_appearance is null then 1
                when days_since_previous_appearance > {{ window_days }} then 1
                else 0
            end
        ) over (
            partition by business_signature
            order by event_date, source_row
            rows between unbounded preceding and current row
        ) as recapture_cluster
    from gapped

)

select
    *,
    row_number() over (
        partition by business_signature, recapture_cluster
        order by event_date, source_row
    ) as recapture_rank,
    min(event_date) over (
        partition by business_signature, recapture_cluster
    ) as surviving_event_date
from clustered
