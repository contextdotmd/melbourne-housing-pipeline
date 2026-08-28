---
status: accepted
---

# DuckDB as the execution engine

The transformation layer runs on dbt-duckdb against a local database file rather than a cloud
warehouse. The whole pipeline reproduces with `make all` in under a minute — no account, no
credentials, no container runtime — and 63k rows rebuild in well under a second, which is what
keeps the red→green test loop fast enough to actually practise TDD.

## Considered options

BigQuery and Snowflake were rejected because a reviewer cannot run the project without
standing up their own project and credentials, and because the feedback loop is far too slow
for test-first work. Postgres in Docker was rejected because Docker is not installed on the
development machine, and a row-store OLTP engine is a poor stand-in for a warehouse anyway.

## Consequences

DuckDB is not a cloud warehouse, so the README owes the reader an explicit porting section.
DuckDB's single-writer file lock also constrains the orchestration design — see ADR-0003.
