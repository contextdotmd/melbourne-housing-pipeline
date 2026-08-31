---
status: accepted
---

# Incremental where volume grows, full rebuild where it does not

The feed is a sequence of daily CSVs. Each may carry new records, records already delivered,
and corrections to records delivered earlier. History accumulates, so a pipeline that rebuilds
everything on every run does unbounded work — the wrong shape regardless of how fast it is
today. This supersedes [ADR-0008](0008-no-incremental-materialisation.md).

Incrementality is applied **per layer, according to what makes that layer grow**:

| Layer | Strategy | Unit of work | Why |
|---|---|---|---|
| `stg__listing_outcome` | `append` | the load | The immutable event log. Loads already staged are skipped, so a re-run is harmless |
| `int__listing_outcome` | table | — | The deliberate exception to the volume rule: it grows with staging, but it is a windowless row-for-row projection (code decode, measure split), so its rebuild is one linear scan. Scoping it by property would need the same affected-property plumbing as the clustered model for no window-correctness gain — complexity spent where nothing was at risk |
| `int__listing_outcome_clustered` | `delete+insert` | **property** | Both dedup rules are windows partitioned by a key containing the property |
| `int__..._deduped`, `quarantine` | view | — | Complementary filters over the model above; they cannot drift and need no logic of their own |
| `fact__listing_outcome` | `delete+insert` | **property** | 1:1 with the deduped view, so the same unit applies |
| `dim_property` | `delete+insert` | **property** | Grows with the event stream — 58,696 rows and rising |
| `dim_suburb`, `dim_agent`, code dims | table | — | 377 and 470 rows. Bounded by the real world, not by the feed |
| `dim_date` | table | — | One row per day; a spine is cheap to regenerate |
| everything in `analytics` | table | — | See below |

## The unit of work is the window's partition key

This is the rule that makes the whole thing decidable rather than a judgement call each time.
A model that computes window functions can only be reprocessed in units that no window spans.
Read the `partition by` and the unit falls out:

- clustering partitions by the business signature, which begins with the property → **property**
- `analytics_repeat_sales` lags within `property_key` → **property**
- `analytics_suburb_monthly_market` lags within `suburb_key` → **suburb**, and the whole
  series, because a changed month ripples forward through every later moving average
- `analytics_agent_performance` computes region share over `(month_start, region_name)` → the
  **region-month**, not the agent: one agent's new sale changes every other agent's share

Verified rather than assumed: `assert_no_dedup_group_spans_two_properties.sql` fails the build
if the clustering signature ever stops containing the property.

## Why the analytics layer is not incremental

It was made incremental, and the equivalence harness immediately caught two bugs that no unit
test would have:

- **Stale denormalised attributes.** An agency's canonical display name is the most frequent
  spelling across all its rows. New data shifted it from `VICProp` to `VICPROP`, but
  region-months not touched by later deliveries kept the old copy.
- **Silent invalidation from a growing spine.** Extending `dim_date` adds months to every
  suburb's grid — including suburbs no delivery touched, which kept their shorter series.
  12,623 rows against 12,818.

Both are the same class: an incrementally-maintained aggregate that copies something able to
change beneath it. Solvable, but only by tracking dependency invalidation across models.

That complexity is not worth buying here, because **these aggregates do not grow with the
event stream**. Their size is bounded by dimension cardinality — suburbs × months, agents ×
region-months — so they stay in the tens of thousands of rows while the fact grows without
limit. Rebuilding 13,000 rows to avoid a class of staleness bug is a good trade.

The principle generalises: **spend incremental complexity only where the row count is driven by
event volume.**

## Why the write is a delete plus an append, not a merge or a `unique_key`

The write replaces a whole property, and that is not an upsert.

`merge` cannot express it. When a later delivery arrives with an outcome identical to one
already in the fact but dated a week earlier, the recapture rule keeps the earliest — so the
row already in the fact must **stop existing** and be replaced by a different row with a
different surrogate key. Demonstrated: fact goes from `2017-01-14` to `2017-01-07`, the key
changes, and the superseded row moves to quarantine. A merge would have left both and reported
two sales where one occurred, because it can only update rows the incoming set matches. It has
no way to remove what no longer qualifies.

dbt's `delete+insert` does the right thing, and its `unique_key` is documented as tolerating a
non-unique column. It is still the wrong thing to write here: the reprocessing unit is the
property, and a property has many rows — 59,816 outcomes across 58,696 properties, one with
four. Declaring that as `unique_key` asserts something untrue, and the next reader either
believes it or switches to `merge` and silently breaks the model.

So the delete is spelled out in a `pre_hook` and the strategy is `append`. The config then says
what actually happens, and uniqueness is asserted where it genuinely holds:

| Model | Real row key | Reprocessing unit |
|---|---|---|
| `stg__listing_outcome` | `(load_id, source_row)` | the load |
| `int__listing_outcome_clustered` | `(load_id, source_row)` | the property |
| `fact__listing_outcome` | `listing_outcome_key` | the property |
| `dim_property` | `property_key` | the property |

Every one of those row keys carries a uniqueness test. On an incremental model that matters
more than usual: a scope predicate that deleted too little shows up immediately as duplicated
rows, rather than as a silent overcount three models downstream.

## The scope is passed in, not inferred

The orchestrator knows what it just ingested and says so:

```bash
dbt build --vars '{"load_ids": ["a1b2c3"]}'
```

Every incremental model derives its scope from that one list, so the warehouse cannot disagree
with itself about what changed. With no var — a manual run, CI, a full refresh — the scope is
everything and each model falls back to full-refresh behaviour. The incremental path must never
be the only way to get a correct answer.

## Consequences

Incremental correctness is not something to reason about once and trust. `make test-equivalence`
replays a realistic delivery sequence — five daily files, one re-sent verbatim, one carrying a
late restatement — and asserts the result is identical to a full rebuild, table by table. It
found both bugs above. Every future change to a partition key or a scope predicate has to keep
it passing.
