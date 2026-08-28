{{ config(materialized='view') }}

-- Classify every row as good, an exact duplicate, a restatement, or a recapture. One
-- expression, so the surviving rows and the quarantined rows are complementary by
-- construction and nothing can fall between them.
--
-- The source duplicates rows in TWO opposite ways, and each needs its own rule:
--
--   restatement  same property, date, method and agent - but the content changed. Almost
--                always a price published after the fact ("sold not disclosed" later becoming
--                a figure). The date is constant and the content varies.
--
--   recapture    same content, different date. The feed stopped updating over the Christmas
--                auction shutdown and republished the 16/12/2017 snapshot four more times,
--                stamped with the capture date. The content is constant and the date varies.
--
-- They are exact opposites, which is why one rule cannot catch both: a signature containing
-- the price is blind to restatements, and one containing the date is blind to recaptures.
--
-- An exact duplicate - the same row twice, changing nothing - is called out separately rather
-- than lumped in with restatements, because nothing was restated and CONTEXT.md draws that
-- distinction.
--
-- Order matters. Restatements are resolved first and excluded from the recapture chain: a
-- duplicate sitting on day one would otherwise become the lag anchor for the following week,
-- and the recapture gap would be measured from the wrong row.

{% set window_days = var('recapture_window_days') %}

with content_hashed as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['rooms', 'type_code', 'sale_price', 'bid_amount']) }}
            as content_signature,
        -- Row identity for the join back. A surrogate key rather than (load_id, source_row)
        -- directly: joining on a nullable column silently drops rows, because NULL = NULL is
        -- false. generate_surrogate_key substitutes a sentinel for nulls, so the join holds.
        {{ dbt_utils.generate_surrogate_key(['load_id', 'source_row']) }} as row_key
    from {{ ref('int__listing_outcome') }}

),

restated as (

    select
        *,
        -- Most informative row wins: a published figure beats a null one. source_row breaks
        -- ties so the choice is deterministic across rebuilds.
        row_number() over restatement_group        as restatement_rank,
        first_value(content_signature) over restatement_group as surviving_content_signature
    from content_hashed
    window restatement_group as (
        partition by suburb_key, street_address_key, event_date, method_code, agent_key
        order by
            case when coalesce(sale_price, bid_amount) is not null then 0 else 1 end,
            source_row
    )

),

signed as (

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
    from restated

),

-- The recapture chain is built over restatement survivors only.
gapped as (

    select
        row_key,
        source_row,
        business_signature,
        event_date,
        date_diff(
            'day',
            lag(event_date) over (
                partition by business_signature
                order by event_date, source_row
            ),
            event_date
        ) as days_since_previous_appearance
    from signed
    where restatement_rank = 1

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

),

chain as (

    select
        row_key,
        days_since_previous_appearance,
        recapture_cluster,
        row_number() over (
            partition by business_signature, recapture_cluster
            order by event_date, source_row
        ) as recapture_rank,
        min(event_date) over (
            partition by business_signature, recapture_cluster
        ) as surviving_event_date
    from clustered

)

-- Left join rather than a union: (load_id, source_row) is unique per input row, so this emits
-- exactly one row per row the loader accepted. That is what keeps the ledger provable.
select
    signed.* exclude (restatement_rank, surviving_content_signature, content_signature, row_key),
    chain.days_since_previous_appearance,
    chain.recapture_cluster,
    chain.recapture_rank,
    coalesce(chain.surviving_event_date, signed.event_date) as surviving_event_date,
    case
        when signed.restatement_rank > 1
             and signed.content_signature = signed.surviving_content_signature
            then 'exact_duplicate'
        when signed.restatement_rank > 1 then 'suspected_restatement'
        when chain.recapture_rank = 1 then 'ok'
        else 'suspected_recapture'
    end as dq_status
from signed
left join chain on signed.row_key = chain.row_key
