"""The data-quality gate.

Kept as a pure function over an ingest receipt so it can be tested without a scheduler, a
warehouse or a filesystem. The Airflow task is a thin wrapper that raises on what this
returns.
"""

from __future__ import annotations

import pytest

from ingest.quality_gate import (
    MAX_REJECT_RATE,
    MAX_ROWS_LOADED,
    MIN_ROWS_LOADED,
    evaluate_receipt,
)


def receipt(**overrides) -> dict:
    base = {
        "load_id": "145c1499c13e",
        "rows_read": 63_023,
        "rows_loaded": 63_023,
        "rows_rejected": 0,
        "reject_reasons": {},
    }
    base.update(overrides)
    return base


def test_a_healthy_load_raises_no_problems():
    assert evaluate_receipt(receipt()) == []


def test_a_ledger_that_does_not_reconcile_is_fatal():
    """The one check that cannot be tuned away: rows must not vanish between read and write."""
    problems = evaluate_receipt(receipt(rows_read=63_023, rows_loaded=60_000, rows_rejected=0))
    assert any("ledger" in p for p in problems)


def test_reject_rate_within_tolerance_passes():
    rows = 63_023
    rejected = int(rows * MAX_REJECT_RATE) - 1
    problems = evaluate_receipt(
        receipt(rows_read=rows, rows_loaded=rows - rejected, rows_rejected=rejected)
    )
    assert problems == []


def test_reject_rate_above_tolerance_is_reported_with_its_reasons():
    """A spike in rejects means the source changed shape — the reasons say how."""
    rows = 63_023
    rejected = int(rows * MAX_REJECT_RATE) + 1
    problems = evaluate_receipt(
        receipt(
            rows_read=rows,
            rows_loaded=rows - rejected,
            rows_rejected=rejected,
            reject_reasons={"date_not_parseable": rejected},
        )
    )
    assert any("reject rate" in p for p in problems)
    assert any("date_not_parseable" in p for p in problems)


def test_a_truncated_source_is_caught():
    problems = evaluate_receipt(
        receipt(rows_read=MIN_ROWS_LOADED - 1, rows_loaded=MIN_ROWS_LOADED - 1)
    )
    assert any("outside the expected" in p for p in problems)


def test_a_duplicated_source_is_caught():
    problems = evaluate_receipt(
        receipt(rows_read=MAX_ROWS_LOADED + 1, rows_loaded=MAX_ROWS_LOADED + 1)
    )
    assert any("outside the expected" in p for p in problems)


def test_an_empty_load_does_not_divide_by_zero():
    """A zero-row file is a volume problem, not a crash."""
    problems = evaluate_receipt(receipt(rows_read=0, rows_loaded=0, rows_rejected=0))
    assert any("outside the expected" in p for p in problems)


def test_every_problem_is_reported_not_just_the_first():
    """An operator fixing one issue should not discover the next on the following run."""
    problems = evaluate_receipt(
        receipt(rows_read=100, rows_loaded=50, rows_rejected=10, reject_reasons={"x": 10})
    )
    assert len(problems) >= 2


@pytest.mark.parametrize("missing", ["rows_read", "rows_loaded", "rows_rejected"])
def test_a_malformed_receipt_fails_loudly(missing):
    """Silently passing a gate because a field was absent is the worst possible outcome."""
    broken = receipt()
    del broken[missing]
    with pytest.raises(KeyError):
        evaluate_receipt(broken)
