-- Guards a decision the pipeline has not made yet.
--
-- A future incremental build reprocesses a bounded window around each affected event rather
-- than a property's entire history, because an unbounded scope cannot prune partitions (see
-- docs/warehouse-physical-design.md). That is only correct while no deduplication cluster
-- spans more than the window: a longer chain would be split, and its tail would survive into
-- the fact as a phantom event.
--
-- Asserting it now, while the build is still full-refresh, means the window is known to be
-- safe before anything relies on it — rather than discovered to be too short afterwards, by
-- which time the damage is silent.

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
