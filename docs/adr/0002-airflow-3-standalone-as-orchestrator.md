---
status: accepted
---

# Airflow 3 standalone as the orchestrator

Orchestration uses Apache Airflow 3 run via `airflow standalone`, installed with `uv` against
the official constraints file for Python 3.12. It needs no container runtime, no Java and no
external metadata database, so it adds a real scheduler without adding infrastructure.

## Consequences

Airflow's dependency tree conflicts with dbt-core's when resolved together, so it lives in a
separate `airflow` optional-dependency group installed into its own environment. Two
environments is the cost of having both tools at current versions.
