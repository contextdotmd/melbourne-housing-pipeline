---
status: accepted
---

# Split `Price` into `sale_price` and `bid_amount`

The source `Price` column means two different things depending on the row: consideration paid
on a sale, or the highest/vendor bid on a property that did not sell. 10,964 rows are in the
second category, so `AVG(price)` over the priced population is roughly 23% contaminated —
returning a plausible, wrong number rather than a NULL.

The fact therefore exposes two mutually exclusive nullable measures and no column named
`price`. `sale_price` is populated only when the outcome is a sale; `bid_amount` only when it
is not.

## Considered options

A single `reported_amount` with an `amount_type` discriminator was rejected: it documents the
hazard rather than removing it, and every consumer would have to remember the filter. Dropping
bid figures entirely was rejected because the gap between vendor expectation and eventual sale
price is one of the more interesting questions this data supports.

## Consequences

The error is now structurally impossible rather than merely tested against — `AVG(sale_price)`
is correct by construction, with no folklore required.
