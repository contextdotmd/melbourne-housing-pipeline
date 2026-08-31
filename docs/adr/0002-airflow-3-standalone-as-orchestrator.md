---
status: accepted
---

# Airflow 3 standalone as the orchestrator

Orchestration uses Apache Airflow 3 run via `airflow standalone`, installed with `uv` against
the official constraints file for Python 3.12. It needs no container runtime, no Java and no
external metadata database, so it adds a real scheduler without adding infrastructure.

## Consequences

Airflow's dependency tree conflicts with dbt-core's when resolved together, so
`install.sh --with-airflow` builds it a separate environment at `.venv-airflow`, pinned by the
official constraints file and carrying the project code's own imports (pyarrow for the ingest
task, requests for the failure notifier). Two environments is the cost of having both tools at
current versions.
