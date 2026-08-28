---
status: accepted
---

# Ingestion is a Python loader writing to a Parquet landing zone

`src/ingest/loader.py` streams the CSV, asserts the 13-column header contract, coerces types
row by row, and writes accepted rows to `data/landing/` as Parquet, unparseable rows to
`data/rejects/` with a reason code, and one `data/ingest_receipt/<load_id>.json` per run. A
malformed row does not fail the load; it is recorded and the load continues.

## Considered options

`dbt seed` was rejected — dbt's own guidance reserves seeds for small static reference data,
and it leaves nowhere to put row-level error handling. DuckDB's `read_csv_auto` as a bare
source was rejected because it offers essentially no error handling: a malformed row either
kills the query or is silently coerced.

## Consequences

The landing zone is the local stand-in for object storage, so porting to S3 or GCS is a path
change rather than a redesign. The receipt is what makes the row ledger provable: `rows_read ==
rows_loaded + rows_rejected` is the assertion that catches silent loss, and the DQ gate reads
that one small file rather than querying the warehouse.
