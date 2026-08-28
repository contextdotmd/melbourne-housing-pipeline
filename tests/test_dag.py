"""The DAG's structure.

A coarse DAG (ADR-0003) has to earn its keep somewhere other than its shape, so these tests
check the things it does own: that it imports at all, that retries back off, that failures
notify, that concurrent runs cannot collide on DuckDB's single-writer file, and that the
quality gate runs after the build rather than beside it.

Skipped when Airflow is not importable — it lives in its own environment because its
transitive pins conflict with dbt-core's. Run them with `make test-dag`.
"""

from __future__ import annotations

import importlib.util
from datetime import timedelta
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow lives in .venv-airflow; run `make test-dag`")

DAG_FILE = Path(__file__).resolve().parents[1] / "airflow" / "dags" / "housing_pipeline.py"


@pytest.fixture(scope="module")
def dag():
    spec = importlib.util.spec_from_file_location("housing_pipeline", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


def test_the_dag_imports_without_error(dag):
    """A DAG that fails to parse is invisible in the UI rather than loudly broken."""
    assert dag.dag_id == "melbourne_housing_pipeline"


def test_every_task_retries_with_exponential_backoff(dag):
    args = dag.default_args
    assert args["retries"] >= 1
    assert args["retry_exponential_backoff"] is True
    assert isinstance(args["retry_delay"], timedelta)
    assert isinstance(args["max_retry_delay"], timedelta)


def test_failures_are_notified(dag):
    assert callable(dag.default_args["on_failure_callback"])


def test_only_one_run_at_a_time(dag):
    """DuckDB permits a single writer — two concurrent runs would deadlock on the file."""
    assert dag.max_active_runs == 1


def test_catchup_is_off(dag):
    """Backfilling a full-refresh pipeline just rebuilds the same tables N times."""
    assert dag.catchup is False


def test_the_tasks_run_in_dependency_order(dag):
    assert set(dag.task_dict) == {"ingest", "dbt_build", "dq_gate"}
    assert dag.get_task("ingest").downstream_task_ids == {"dbt_build"}
    assert dag.get_task("dbt_build").downstream_task_ids == {"dq_gate"}
    assert dag.get_task("dq_gate").downstream_task_ids == set()


def test_the_quality_gate_runs_after_the_build_not_beside_it(dag):
    """Gating in parallel with the build would let a bad load publish before it is caught."""
    assert "dbt_build" in dag.get_task("dq_gate").upstream_task_ids


def test_notification_is_a_no_op_without_a_webhook(monkeypatch):
    """The pipeline must be runnable by anyone with no configuration at all."""
    spec = importlib.util.spec_from_file_location("housing_pipeline_notify", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    module.notify_failure({"ti": None, "dag_run": None})  # must not raise


def test_notification_never_raises_even_when_the_webhook_fails(monkeypatch):
    """A broken notifier must not mask the failure it was called to report."""
    spec = importlib.util.spec_from_file_location("housing_pipeline_notify2", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    monkeypatch.setenv("SLACK_WEBHOOK_URL", "http://127.0.0.1:1/does-not-exist")
    module.notify_failure({"ti": None, "dag_run": None})  # must not raise
