# How incrementality would actually work

Companion to [ADR-0008](adr/0008-no-incremental-materialisation.md), which explains why the
pipeline is full-refresh today. This is the design for the day that stops being true.

## The short version

Three changes, in this order:

1. **Detect the delta at ingestion**, by row hash — not in dbt. With a full-snapshot feed
   there is nothing for an incremental model to skip, so this is the change that unlocks the
   other two.
2. **Reprocess by property, not by date window.** Both deduplication rules partition by a key
   that contains the property, so a property's rows never interact with another property's.
   Pulling a touched property's *entire* history is therefore always correct — and it removes
   the look-back window completely, which is otherwise the one parameter that can silently be
   set too short.
3. **`delete+insert` on the affected properties**, so a reprocessed property is replaced
   wholesale rather than merged row by row.

Sizing on this data: a median auction day touches **570 of 58,696 properties — under 1% of
history**. The busiest month touches 5.4%.

## Why the obvious approach is wrong

The natural incremental filter is a rolling window on the event date:

```sql
{% if is_incremental() %}
where event_date >= (select max(event_date) - interval 90 day from {{ this }})
{% endif %}
```

This is not merely imperfect here, it is **actively incorrect**, because of restatements. A
restatement *arrives* today but carries the *original* event date — a sale from two years ago
having its price published. An event-time window never sees it, so the correction is applied
never, and the duplicate lives in the fact permanently. Silently: nothing fails.

The general form of the trap: **filter on the time a row arrived, not the time the event
happened.** They are the same thing only in feeds that never restate, and this feed restates.

## Why property is the correct reprocessing unit

Both rules partition by keys that contain the property:

| Rule | Partition |
|---|---|
| restatement | `suburb_key, street_address_key, event_date, method_code, agent_key` |
| recapture | `business_signature` = hash of `suburb_key, street_address_key, rooms, type_code, method_code, agent_key, sale_price, bid_amount` |

Property appears in both, so no deduplication group can span two properties. That is provable
by inspection and verified against the data: **0 groups span more than one property.**

The consequence is the useful part. Reprocessing a whole property means every window function
sees a complete partition, so there is no look-back to size, no margin to guess, and no way to
set it too short. The correctness of the incremental build stops depending on a tuned number.

## The mechanism

### 1. Delta detection at ingestion

The loader already hashes the file. It would additionally hash each row and compare against
the previous load's hashes:

```python
row_hash = sha256(f"{suburb}|{address}|{rooms}|…|{council_area}")

if row_hash in previous_load_hashes:
    classification = "unchanged"      # not written to the delta
elif natural_key in previous_load_keys:
    classification = "restated"       # same property/date/method, new content
else:
    classification = "new"
```

Written as `data/landing/listing_outcome/load_id=<id>/` containing only `new` and `restated`
rows, with the counts added to the ingest receipt so the quality gate can alarm on an
implausible delta — a sudden 100% delta means the hash basis changed, not that the market did.

This is the step that matters. Without it, the "affected properties" set is every property,
and the incremental build degenerates into a slower full refresh.

### 2. The dbt side

```sql
{{ config(
    materialized='incremental',
    unique_key='listing_outcome_key',
    incremental_strategy='delete+insert',
) }}

with affected_properties as (

    select distinct suburb_key, street_address_key
    from {{ ref('stg__listing_outcome') }}
    {% if is_incremental() %}
    where load_id not in (select distinct load_id from {{ this }})
    {% endif %}

),

-- Full history for the touched properties, so every window sees a complete partition.
scope as (

    select history.*
    from {{ ref('stg__listing_outcome') }} as history
    {% if is_incremental() %}
    inner join affected_properties using (suburb_key, street_address_key)
    {% endif %}

)

-- …restatement ranking, clustering and classification, exactly as today, over `scope`.
```

`delete+insert` keyed on the property (not the row) is what makes it idempotent: the whole
property is removed and rebuilt, so a row that *should* now be quarantined actually disappears
from the fact. A row-level merge would leave it behind — the classic incremental bug where
deletes never propagate.

### 3. What the quarantine needs

Today the quarantine is rebuilt wholesale. Incrementally it needs the same
delete-by-property treatment, or a row reclassified from `suspected_recapture` to `ok` would
exist in both tables and the row ledger would break. The ledger test is what would catch it,
which is the argument for keeping that test in the incremental path.

## Tests that would guard it

The existing suite mostly carries over; three additions are specific to incrementality:

1. **Two-load idempotency.** Load the file, build. Load the identical file again, build. The
   fact must be byte-identical and the ledger must still balance.
2. **Restatement across loads.** Load A with an undisclosed sale, load B restating it with a
   price. The fact must hold one row, with the price — this is the test that fails under an
   event-date window and is the whole reason for the design.
3. **Late relisting reopens a cluster.** Load A with an outcome, load B with an identical one
   eight days later. The second must be quarantined as a recapture, proving the reprocessing
   scope pulled load A's row back in.

## When to switch it on

Not on row count — on build time against the schedule. At roughly 21,000 events a year this
dataset would take decades to become inconvenient, and a national feed at ten times the volume
still rebuilds in seconds on a real warehouse.

The honest trigger is: the full build no longer fits the window the business needs it in. Until
then, incrementality trades a correctness guarantee that currently comes free for a performance
gain nobody is asking for.
