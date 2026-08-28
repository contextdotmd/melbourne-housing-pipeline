---
status: accepted
---

# No incremental materialisation

Every model is rebuilt in full on each run. At 63,023 rows the entire build completes in well
under a second, so incrementality would buy nothing while introducing watermark logic, merge
keys and the class of bug where a strict `>` comparison silently drops same-day arrivals.

## Consequences

This will not hold at production volume. The README documents how to add incrementality —
merge on the fact's surrogate key with a rolling look-back window rather than a strict
watermark — so the omission reads as a scoping decision rather than an oversight.
