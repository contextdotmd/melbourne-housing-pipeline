---
status: accepted
---

# The fact grain is the listing outcome, not the sale

`fact__listing_outcome` holds one row per marketing or auction campaign for one property on one
event date — sold or not. Of 63,023 rows, 16,230 are not sales at all (`Method` ∈ {PI, VB, W}),
so a table named `sales` would assert something false about a quarter of its own contents and
make every naive aggregate wrong.

Keeping non-sales also makes clearance rate — sales divided by all outcomes: 74.2% at source
grain, 74.3% in the fact once republications are collapsed — computable at all. It is the headline measure of this market, and filtering to sold
rows destroys the denominator.

## Consequences

`is_sold` is derived from `Method` and lives on `dim_sale_method`, since it is a pure function
of the code. Whether a price was disclosed is *not* a function of the code (`S` is 10.1% null,
`PI` only 39.3%) so `price_is_disclosed` is a row-level column on the fact.
