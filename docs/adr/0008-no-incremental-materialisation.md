---
status: accepted — conditional on the feed being a cumulative snapshot
---

# Full refresh, because this feed is a cumulative snapshot

Every model is rebuilt in full on each run. At 63,023 rows the whole warehouse builds in about
11 seconds, so incrementality would buy nothing on compute while introducing watermark logic,
merge keys, and the class of bug where a strict `>` comparison silently drops arrivals.

**This decision depends on the delivery pattern, not on the row count.** It is recorded that way
deliberately: the row count is what people usually cite, and it is the weaker reason.

## What we know about the feed, and how

We were given a file, not a contract, so the pattern was inferred from the data:

- **It is a cumulative snapshot, republished periodically.** The re-scrape artefact
  (ADR-0006) is the proof — the same rows reappearing across five consecutive publications
  under new capture dates is something a delta feed cannot produce.
- **Rows are not immutable.** Restatements are present within a single snapshot: a sale
  recorded once as "not disclosed" and again with a figure.

## Why that particular pattern favours full refresh

Both deduplication rules are window functions over history. Under a cumulative snapshot, a full
refresh re-derives them from the file every run, so a correction published today is applied to
a two-year-old event automatically and for free.

An incremental model would not inherit that. Worse, the natural filter is actively wrong: a
restatement *arrives* now but carries an *old* event date, so an event-time window never sees
it and the duplicate persists permanently, silently. Getting it right requires reprocessing by
affected key — real work, done to reproduce something full refresh already does.

There is also a prerequisite that is easy to miss. Under a cumulative snapshot **every row
arrives every day**, so "rows changed since last run" is the entire file until something
computes the delta. An incremental model bolted on without that degenerates into a slower full
refresh.

## If the feed pattern changes, so does this decision

| Feed pattern | Right answer | Why |
|---|---|---|
| **Cumulative snapshot** (what we have) | Full refresh | Restatement correctness comes free; a delta must be computed before incrementality means anything |
| **Daily delta** — each file holds only new and corrected records | **Incremental, from the start** | The delta is already computed by the vendor, so the hard prerequisite disappears. History grows without bound while the daily delta stays flat, and full refresh over ever-growing history is the wrong shape even while it remains fast |
| **Rolling window** — each file holds the last N days, overlapping | Incremental, scoped to the window | A hybrid: dedupe the overlap on arrival, then treat as a delta |

Under a delta feed the correctness argument above **does not transfer**. Full refresh only
inherits restatements automatically because today's snapshot contains them; a delta feed
delivers the restatement as a new row against an old event date either way. The advantage
disappears and only the cost remains — so the recommendation inverts.

Note that volume would still not force the change: at roughly 21,000 events a year, a decade of
history is 210,000 rows, which rebuilds in about two seconds. The argument for building
incrementally under a delta feed is not speed. It is that unbounded work per run is the wrong
shape, and retrofitting it onto ten years of accumulated history costs more than building it in.

**One thing does not change with the feed pattern:** reprocessing must be scoped by *property*,
never by an event-date window. Both dedup rules partition by a key containing the property, so a
property's rows never interact with another property's — which makes whole-property reprocessing
provably safe and removes the look-back window entirely. See
[docs/incremental-design.md](../incremental-design.md).

## The question to ask before revisiting

Not "how many rows are there?" but **"what does the source actually send, and can it restate?"**
Those two answers determine the design. Everything else is sizing.

## Consequences

The README documents this so the omission reads as a scoping decision rather than an oversight.
The risk carried is that build time grows unnoticed; the mitigation is that it is measured on
every run and recorded in the ingest receipt.

Reference dimensions loaded from seeds (`seed_sale_method`, `seed_property_type`) stay full
refresh under every pattern above: they are small, hand-maintained reference data, and
incrementality on ten rows is pure ceremony.
