-- Guards a design the DuckDB pipeline does not use yet.
--
-- The shipped incremental build reprocesses a property's entire history, so no look-back
-- window exists to get wrong. The BigQuery port cannot afford that: an unbounded scope cannot
-- prune partitions, so it reprocesses a bounded window around each affected event instead
-- (see docs/warehouse-physical-design.md). That is only correct while no deduplication
-- cluster spans more than the window: a longer chain would be split, and its tail would
-- survive into the fact as a phantom event.
--
-- Asserting it on every build here means the window is known to be safe before anything
-- relies on it — rather than discovered to be too short afterwards, by which time the
-- damage is silent.

select
    business_signature,
    recapture_cluster,
    count(*)                                                     as rows_in_cluster,
    min(event_date)                                              as cluster_start,
    max(event_date)                                              as cluster_end,
    date_diff('day', min(event_date), max(event_date))           as cluster_span_days
from {{ ref('int__listing_outcome_clustered') }}
where dq_status in ('ok', 'suspected_recapture')
group by business_signature, recapture_cluster
having date_diff('day', min(event_date), max(event_date)) > {{ var('incremental_lookback_days') }}
