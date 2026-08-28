---
status: accepted
---

# TDD the logic, contract-test the plumbing

Every transformation carrying real derivation logic is written test-first: the loader, the
`Method` → `is_sold` mapping, the `sale_price`/`bid_amount` split, the recapture collapse, date
parsing, and every window function in the analytics layer. dbt unit tests make this genuine
red→green for SQL, since they run against mocked rows with no warehouse data required.

Dimensions and passthrough columns get schema and contract tests instead — a `SELECT DISTINCT`
has no logic to get wrong, and unit-testing it is ceremony rather than discipline.

## Consequences

The failing test and its implementation are committed separately, so the history evidences the
practice rather than merely asserting it.
