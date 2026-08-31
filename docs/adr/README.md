# Architecture decision records

One file per decision, written before the code that implements it. Each records the decision,
the alternatives genuinely considered, and the consequences accepted. A superseded ADR is kept,
marked, and linked to its successor — the reversal is part of the record.

| # | Decision | Status |
|---|---|---|
| [0001](0001-duckdb-as-execution-engine.md) | DuckDB as the execution engine | accepted |
| [0002](0002-airflow-3-standalone-as-orchestrator.md) | Airflow 3 standalone as the orchestrator | accepted |
| [0003](0003-airflow-owns-phases-dbt-owns-lineage.md) | Airflow owns phases, dbt owns the model graph | accepted |
| [0004](0004-fact-grain-is-the-listing-outcome.md) | The fact grain is the listing outcome, not the sale | accepted |
| [0005](0005-split-price-into-sale-price-and-bid-amount.md) | Split `Price` into `sale_price` and `bid_amount` | accepted |
| [0006](0006-collapse-rescrape-recaptures.md) | Collapse duplicated rows under three named reasons, quarantine the evidence | accepted |
| [0007](0007-tdd-the-logic-contract-test-the-plumbing.md) | TDD the logic, contract-test the plumbing | accepted |
| [0008](0008-no-incremental-materialisation.md) | Full refresh, because this feed is a cumulative snapshot | superseded by 0011 |
| [0009](0009-python-loader-to-parquet-landing.md) | Ingestion is a Python loader writing to a Parquet landing zone | accepted |
| [0010](0010-surrogate-keys-via-dbt-utils.md) | Surrogate keys via `dbt_utils.generate_surrogate_key` | accepted |
| [0011](0011-incremental-by-layer.md) | Incremental where volume grows, full rebuild where it does not | accepted |
| [0012](0012-landing-partition-as-ingestion-commit-marker.md) | The landing partition is ingestion's commit marker | accepted |

The design these decisions compose into is [docs/tech-design.md](../tech-design.md).
