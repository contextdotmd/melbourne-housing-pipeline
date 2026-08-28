---
status: accepted
---

# No incremental materialisation

Every model is rebuilt in full on each run. At 63,023 rows the entire warehouse builds in
about 11 seconds, so incrementality would buy nothing on compute while introducing watermark
logic, merge keys, and the class of bug where a strict `>` comparison silently drops same-day
arrivals.

## What the production feed actually looks like

Not a guess — the data says so:

- **It is a full-file snapshot, republished periodically.** The re-scrape artefact
  (ADR-0006) is the proof: the same rows reappeared across five consecutive publications with
  new capture dates. A feed that only appended deltas could not produce that.
- **It is cumulative.** Each publication carries the whole history plus new events.
- **Rows are not immutable.** Restatements are present in a single snapshot — a sale recorded
  once as "not disclosed" and again with a figure. Content changes after first publication.

So the pattern is: *full cumulative snapshot, with appends and in-place restatements.*

## Why that pattern argues for full refresh, not against it

The obvious reading is that a cumulative snapshot is wasteful to reprocess. The less obvious
one matters more: **restatement makes full refresh the more correct choice, not merely the
cheaper one.**

Both deduplication rules are window functions over history. Recapture clustering needs the
previous appearance of the same signature; restatement resolution needs every row sharing
`(property, date, method, agent)`. A full refresh re-derives both from the current snapshot
every run, so a correction arriving today is applied to an event from two years ago
automatically.

An incremental model would not get that for free. The natural filter — "rows whose
`event_date` is within the last N days" — is exactly wrong, because a restatement arrives
*now* but carries an *old* event date. Filtering on event time would never see it, and the
duplicate would persist in the fact permanently.

Doing it correctly means reprocessing by *affected key* rather than by date: collect the
groups and signatures touched by the incoming load, pull their full history back in, re-derive,
and merge. That is a genuine engineering exercise, and it exists to reproduce something full
refresh already does.

## When to revisit, and how

Measured inputs for sizing a look-back, should it ever be needed: the longest collapsed
cluster spans **23 days** and holds at most **5 rows**; 20 signatures span more than one
cluster. A 90-day look-back would carry a wide margin.

Revisit when the build stops fitting the schedule — not when the row count merely looks large.
At roughly 21,000 events per year, this dataset would take decades to become inconvenient, and
a national feed at ten times the volume still rebuilds in seconds on a real warehouse.

The first thing to make incremental is **ingestion**, not the models: re-parsing a large source
file every run is the real waste. That is already half-solved — `load_id` is the source
SHA-256, so an unchanged file is detectable as a no-op.

If the fact must eventually go incremental:

1. `materialized='incremental'`, `unique_key='listing_outcome_key'`,
   `incremental_strategy='delete+insert'`.
2. Select the affected keys from the incoming load, **not** a date window.
3. Reprocess the full history of those keys so the window functions see complete groups.
4. Keep the row-ledger test, which is what would catch a look-back that is too short.

## Consequences

The README documents this so the omission reads as a scoping decision rather than an oversight.
The risk carried is that the build time grows silently; the mitigation is that it is measured
on every run and reported in the ingest receipt.
