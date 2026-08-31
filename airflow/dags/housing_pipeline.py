"""Melbourne housing pipeline.

The DAG is deliberately coarse — `ingest → dbt_build → dq_gate`, with failure notification
as a task-level callback — rather than one Airflow task per dbt model (ADR-0003). dbt already derives the model graph from `ref()`;
mirroring it here would maintain the same graph twice, and only one copy fails loudly when
they diverge. On DuckDB it would also be illusory: per-model tasks are separate processes and
DuckDB permits a single writer, so they would have to run through a one-slot pool anyway.

What Airflow owns instead is the operational envelope — schedule, retries with backoff,
failure notification, and a data-quality gate that fails the run before anything downstream
consumes a bad build.
"""

from __future__ import annotations

import json
import os
import sys
import textwrap
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.bash import BashOperator

# The repo root, resolved from this file so the DAG works wherever AIRFLOW_HOME points.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingest.quality_gate import evaluate_receipt  # noqa: E402


def notify_failure(context) -> None:
    """Post a one-line failure summary to Slack.

    No-ops when SLACK_WEBHOOK_URL is unset, so the pipeline is runnable by anyone without
    configuration. Never raises: a broken notifier must not mask the failure it is reporting.
    """
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook:
        return

    task_instance = context.get("ti")
    dag_run = context.get("dag_run")
    message = textwrap.dedent(
        f"""
        :x: *Melbourne housing pipeline failed*
        • task: `{getattr(task_instance, "task_id", "unknown")}`
        • run: `{getattr(dag_run, "run_id", "unknown")}`
        • logs: {getattr(task_instance, "log_url", "n/a")}
        """
    ).strip()

    try:
        import requests

        requests.post(webhook, json={"text": message}, timeout=10)
    except Exception:  # noqa: BLE001 - a failing notifier must not hide the real failure
        pass


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "on_failure_callback": notify_failure,
}

with DAG(
    dag_id="melbourne_housing_pipeline",
    description="Ingest the Melbourne housing CSV, build the warehouse, gate on data quality.",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,  # DuckDB is single-writer: two concurrent runs would deadlock on the file
    tags=["dbt", "duckdb", "housing"],
) as dag:

    @task(task_id="ingest")
    def ingest() -> str:
        """Load today's CSV and return its load id.

        The id is what scopes the build downstream, so it is a task return value rather than
        something dbt has to rediscover. Idempotent: the id is the source SHA-256, so
        re-running against identical bytes is a no-op returning the original receipt.
        """
        from ingest.loader import load_csv

        result = load_csv(
            PROJECT_ROOT / "data" / "raw" / "MELBOURNE_HOUSE_PRICES_LESS.csv",
            PROJECT_ROOT / "data",
        )
        return result.load_id

    load_id = ingest()

    dbt_build = BashOperator(
        task_id="dbt_build",
        cwd=str(PROJECT_ROOT),
        # A single `{` is literal in Jinja — doubling the JSON braces breaks the render.
        # A lost XCom renders as the literal string "None"; the guard fails the task
        # rather than scoping dbt to a load that does not exist.
        bash_command=(
            "LOAD_ID='{{ ti.xcom_pull(task_ids=\"ingest\") }}'\n"
            'if [ -z "$LOAD_ID" ] || [ "$LOAD_ID" = None ]; then\n'
            "    echo 'no load_id from the ingest task — was its XCom cleared?' >&2\n"
            "    exit 1\n"
            "fi\n"
            "uv run dbt build --project-dir dbt --profiles-dir dbt --fail-fast "
            '--vars "{\\"load_ids\\": [\\"$LOAD_ID\\"]}"'
        ),
        doc_md=(
            "Runs every model and every test in dependency order, scoped to the load just "
            "ingested. Models that scale with event volume reprocess only the properties "
            "that load touched; aggregates bounded by dimension cardinality rebuild in full "
            "(ADR-0011). `dbt build` interleaves tests with models, so a failing test stops "
            "its dependents rather than letting them build on bad data."
        ),
    )

    @task(task_id="dq_gate")
    def dq_gate() -> dict:
        """Fail the run on an unhealthy load.

        The rules live in ingest.quality_gate as a pure function so they are testable without
        a scheduler; this task only locates this run's receipt and raises on the verdict.
        """
        from airflow.sdk import get_current_context

        run_load_id = get_current_context()["ti"].xcom_pull(task_ids="ingest")
        if not run_load_id:
            raise ValueError("no load_id from the ingest task — did it run?")

        receipt_path = PROJECT_ROOT / "data" / "ingest_receipt" / f"{run_load_id}.json"
        if not receipt_path.exists():
            raise ValueError(f"no ingest receipt for load {run_load_id} — did the ingest task run?")

        receipt = json.loads(receipt_path.read_text())
        problems = evaluate_receipt(receipt)
        if problems:
            raise ValueError("data quality gate failed: " + "; ".join(problems))

        return {
            "load_id": receipt["load_id"],
            "rows_loaded": receipt["rows_loaded"],
            "rows_rejected": receipt["rows_rejected"],
        }

    load_id >> dbt_build >> dq_gate()
