---
status: accepted
---

# The landing partition is ingestion's commit marker

Every loader output is written under a process-unique `.inprogress` name and renamed into
place — the rejects file first, then the receipt, then the landing partition last. A load
exists exactly when its landing partition does: dbt's landing glob reads nothing else, so a
crash at any point leaves either no visible load or a complete one, and every partial state
re-runs cleanly.

The trap this closes is not the obvious half-written file. pyarrow finalises a valid footer
when a writer exits on an exception, so a crashed load leaves a *readable* partial partition
if written at its final path — with no receipt for the quality gate to evaluate, silently
unioned into the append-only staging log by the landing glob, and never overwritten again
because the repaired source hashes to a different `load_id`.

Re-running identical bytes under the same loader version is a no-op that returns the original
receipt: `started_at` orders loads for restatement survivorship, and re-stamping it would let
a full refresh disagree with the incremental warehouse about which row of a restatement group
is current. A loader-version change reprocesses instead, so new coercion rules are never
bypassed by a partition the old loader wrote.

## Considered options

The receipt as the commit marker was rejected: staging LEFT JOINs the receipt, so its absence
gates nothing — a landing partition without one still stages, just with a null
`load_started_at` that silently loses restatement survivorship (survivors sort nulls last).
Renaming the landing file last removes the race instead of testing for it; staging's
`not_null` test on `load_started_at` stands behind it as the backstop.

A lock file for concurrent ingests was rejected as more machinery than the risk warrants:
pid-suffixed temp names already stop two concurrent ingests of one file interleaving writes
into one temp file, and the final rename is atomic either way.

## Consequences

`LOADER_VERSION` participates in the no-op check, so it must be bumped whenever coercion
behaviour changes — an unbumped version silently reuses partitions the new rules would have
rejected. The receipt is demoted from commit marker to audit record; the invariant "every
landing partition has a receipt" is enforced by the rename order and asserted every build.
