# Physical design: partitioning, clustering, and old records

Companion to [incremental-design.md](incremental-design.md). That covers *what* to reprocess;
this covers how the tables are laid out, and what a 1980 sale record does to all of it.

## Where this matters

DuckDB has no partitioning or clustering in the BigQuery or Snowflake sense — it keeps min/max
zone maps per row group automatically, and the landing zone is already Hive-partitioned by
`load_id`. So this is a porting concern, and the design below is what the star becomes on a
cloud warehouse.

## The layout

| Table | Partition | Cluster | Why |
|---|---|---|---|
| `fact__listing_outcome` | `DATE_TRUNC(event_date, MONTH)` | `suburb_key, property_key` | Almost every query is time-bounded; suburb is the primary analysis grain and property is the join key for repeat-sale work |
| `quarantine_recaptured_listing` | `DATE_TRUNC(event_date, MONTH)` | `reason` | Investigated by reason and period, never scanned whole |
| `dim_property` (58,696) | none | `suburb_key` | Too small to partition; clustering helps the suburb-scoped joins |
| `dim_suburb` / `dim_agent` / code dims | none | none | Hundreds of rows — any physical design is overhead |
| `dim_date` | none | none | One row per day; broadcast-joined |
| `analytics_suburb_monthly_market` | `month_start` | `region_name, suburb_key` | Consumers filter to a period and a region |

**Monthly, not daily.** BigQuery caps a table at 4,000 partitions. Daily partitions exhaust
that after about 11 years — and this is exactly the case a 1980 record creates. Monthly gives
333 years of headroom. Given ~570 outcomes on a busy day, daily partitions would also be far
too small to be worth the metadata.

## What a 1980 record actually breaks

Three distinct problems, only one of which is about scanning.

### 1. It may not be a record at all

`30/12/1980` parses perfectly, and so does `30/12/2087`. Before this was addressed, both loaded
silently — typing validates *form*, never *plausibility*.

The loader now rejects `date_in_future` and `date_implausible` (before 1900), and the ingest
receipt records `event_date_min` and `event_date_max` so a feed that suddenly reaches back
forty years is visible rather than silent. A genuinely old date is still loaded: the point is
to catch corruption, not to refuse history.

### 2. Queries are fine; the incremental *delete* is not

Partition pruning handles reads: a query for 2017 never touches the 1980 partition.

The trap is on the write side. Reprocessing is scoped by **property**, but the table is
partitioned by **event_date**. A `delete ... where property_key in (…)` cannot prune on a
column that is not the partition key, so it scans every partition — including 1980 — on every
run. The incremental build would be *slower* than the full refresh it replaced, and the cost
would grow with history.

The fix is to carry a partition predicate alongside the key predicate, via dbt's
`incremental_predicates`:

```sql
{{ config(
    materialized='incremental',
    unique_key='listing_outcome_key',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'event_date', 'data_type': 'date', 'granularity': 'month'},
    cluster_by=['suburb_key', 'property_key'],
    incremental_predicates=[
        "DBT_INTERNAL_DEST.event_date >= (select min(event_date) from affected_scope)",
        "DBT_INTERNAL_DEST.event_date <= (select max(event_date) from affected_scope)",
    ],
) }}
```

Now the delete touches only the months the incoming delta reaches into. A restatement of a
1980 sale rewrites one 1980 partition, not the table.

### 3. Which forces a bound on the reprocessing scope

[incremental-design.md](incremental-design.md) argues for pulling a touched property's *entire*
history, because it removes the look-back window. That is correct, and on a partitioned table
it is also unprunable: a property with a 1980 sale and a 2026 sale spans 46 years of partitions.

The bound comes from how the rules actually reach:

- **Restatement** partitions by `(property, event_date, method, agent)` — same date. Reach: **0 days.**
- **Recapture** chains only through gaps of `recapture_window_days` (21) or less. Reach: as far
  as the chain runs.

So a row can only interact with rows inside its own cluster's span. Measured on this data, the
**longest cluster spans 23 days** across 5 rows. Reprocessing `[event_date − W, event_date + W]`
per affected property is therefore correct for any W comfortably above that.

`incremental_lookback_days` is set to **90**, and — the part that matters —
`assert_no_cluster_exceeds_lookback.sql` fails the build if any cluster ever grows past it. The
parameter polices itself, so it cannot silently become too short. The shipped DuckDB build
reprocesses whole properties and does not depend on it; asserting it on every build anyway
means the number is known safe *before* the partition-pruned port relies on it.

## Two things that grow badly with a long history

- **`dim_date`** is generated from the data's own span. A 1980 record expands it from 1,035
  rows to ~17,000. Harmless in itself.
- **`analytics_suburb_monthly_market`** is a full suburb × month grid, so it grows as
  `suburbs × months` — 377 × 34 = 12,818 today, but 377 × 552 = 208,000 across 46 years, and
  most of it zero-activity rows for suburbs that did not exist yet. If history ever reaches
  back decades, bound the grid to a rolling reporting window, or to each suburb's own active
  span, rather than the whole calendar. The grid exists to keep `LAG` honest across gaps, and
  a five-year window serves that just as well as a fifty-year one.

## What stays true on DuckDB

The landing zone is already partitioned by `load_id`, so a re-run overwrites one directory. The
star is small enough that DuckDB's automatic zone maps do the pruning a partition scheme would.
None of the above is wasted, though: the reprocessing bound and its assertion are engine-neutral
and are the part that is genuinely hard to retrofit.
