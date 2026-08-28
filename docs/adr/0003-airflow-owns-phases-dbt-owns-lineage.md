---
status: accepted
---

# Airflow owns phases, dbt owns the model graph

The DAG is deliberately coarse — `ingest → dbt_build → dq_gate → notify` — rather than
exploding each dbt model into its own Airflow task. dbt already derives the model-level
dependency graph from `ref()`; mirroring it into Airflow would maintain the same graph in two
places, and only one of them fails loudly when they diverge.

The decisive reason is engine-specific: per-model Airflow tasks run as separate processes, and
DuckDB permits a single writer — a second process cannot even connect, failing with
`IO Error: Could not set lock on file`. A fine-grained DAG would therefore have to run through
a one-slot pool, paying for a large graph while delivering strictly serial execution.

## Considered options

`astronomer-cosmos` renders a dbt project into per-model Airflow tasks and is the
industry-standard answer to this problem. It was rejected here only because of the DuckDB lock;
on BigQuery or Snowflake the trade-off inverts and Cosmos becomes the right choice. This is
recorded in the porting notes.

## Consequences

Because the DAG is small, it must visibly carry retries with backoff, an `on_failure_callback`
notification, a blocking data-quality gate and demonstrable idempotency — otherwise it reads as
a checkbox rather than a design. Task-level retry granularity is given up; at sub-second build
times that costs nothing.
