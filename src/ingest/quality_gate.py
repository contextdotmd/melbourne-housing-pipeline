"""Decide whether an ingest run is healthy enough to publish.

A pure function over the ingest receipt: no scheduler, no warehouse, no filesystem. The
Airflow task is a thin wrapper that raises on whatever this returns, which is what makes the
rules testable in isolation.

It reads the receipt rather than querying the warehouse because the receipt is one small JSON
file, needs no database connection, and is the same artefact the dbt row-ledger test
reconciles against — one source of truth for "what did this load actually do".
"""

from __future__ import annotations

#: A run is unhealthy above this share of unparseable rows. The loader tolerates bad rows by
#: design; a spike in them means the source changed shape rather than that one row is odd.
MAX_REJECT_RATE = 0.01

#: Volume guards against a truncated or duplicated source. The known extract is 63,023 rows.
MIN_ROWS_LOADED = 50_000
MAX_ROWS_LOADED = 200_000


def evaluate_receipt(receipt: dict) -> list[str]:
    """Return every problem with this load. An empty list means healthy.

    Raises KeyError if the receipt is missing a required field — a gate that passes because a
    field was absent is worse than no gate at all.
    """
    rows_read = receipt["rows_read"]
    rows_loaded = receipt["rows_loaded"]
    rows_rejected = receipt["rows_rejected"]

    problems: list[str] = []

    # The ledger check is deliberately not tunable: rows must not disappear between being
    # read and being written, whatever the tolerances are set to.
    if rows_loaded + rows_rejected != rows_read:
        problems.append(
            f"row ledger does not reconcile: "
            f"{rows_loaded:,} loaded + {rows_rejected:,} rejected != {rows_read:,} read"
        )

    reject_rate = rows_rejected / rows_read if rows_read else 0.0
    if reject_rate > MAX_REJECT_RATE:
        reasons = receipt.get("reject_reasons", {})
        problems.append(
            f"reject rate {reject_rate:.2%} exceeds {MAX_REJECT_RATE:.2%} "
            f"({rows_rejected:,} of {rows_read:,}) — reasons: {reasons}"
        )

    if not MIN_ROWS_LOADED <= rows_loaded <= MAX_ROWS_LOADED:
        problems.append(
            f"rows loaded {rows_loaded:,} outside the expected band "
            f"{MIN_ROWS_LOADED:,}–{MAX_ROWS_LOADED:,}"
        )

    return problems
