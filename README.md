# YottaFlex.ai Round 4 — PySpark Performance Optimization

## Goal
Process synthetic retail transactions with PySpark and compare a baseline join/aggregation with an optimized version using repartitioning, broadcast joins, and partitioned Parquet output.

## Architecture
`CSV sources -> explicit-schema ingestion -> validation/quarantine -> accepted facts -> baseline + optimized joins/aggregations -> Parquet outputs + JSON performance report`

## Data model and rules
- `customers`: one row per `customer_id`; customer_id is the primary key.
- `transactions`: one row per `transaction_id`; customer_id references customers.
- Amount must parse as decimal(18,2) and be positive. Supported currencies: INR, USD, EUR.
- event_ts is ISO-8601 UTC. Different currencies are never summed together.
- Duplicate transaction IDs, missing required fields, malformed/non-positive amounts, unsupported currencies, invalid timestamps, duplicate dimension keys, and unknown customer references are rejected with reason and source_file.
- Synthetic input uses a fixed random seed. Duplicate samples and invalid cases are deliberately generated.

## Setup
Use Python 3.10 or 3.11 and a compatible JDK (Java 8/11/17 for Spark 3.5). Ensure `java -version` works.
Windows PowerShell:
`py -m venv .venv`
`.\.venv\Scripts\Activate.ps1`
`python -m pip install --upgrade pip`
`pip install -r requirements.txt`

## Generate data
Small: `python -m src.data_generator --rows 1000 --customers 100 --seed 42`
Large: `python -m src.data_generator --rows 100000 --customers 5000 --seed 42`

## Run
`python -m src.pipeline --transactions data/input/transactions.csv --customers data/input/customers.csv --output-dir data/output --report-dir reports --shuffle-partitions 8`

## Outputs
- data/output/accepted: accepted records in Parquet
- data/output/rejected: rejected records with reject_reason, source_file, run_id
- data/output/baseline: baseline aggregated Parquet
- data/output/optimized: optimized aggregated Parquet partitioned by city
- reports/performance_report.json: counts, timings, speedup, run_id, equivalence and reconciliation
- reports/failure_report.json: failure details when a run fails

## Performance comparison
Baseline uses a normal join and groupBy. Optimized repartitions facts by customer_id, broadcasts the smaller customer dimension, and partitions output by city. Broadcast avoids shuffling the dimension side. Repartitioning and caching have costs; actual results depend on data size, machine, Spark config, and storage. Do not assume optimized is faster for small datasets. Both result sets are compared using exceptAll in both directions.

## Idempotency and recovery
Outputs use overwrite mode at deterministic locations, so reruns replace rather than append output files. Each run has a unique run_id. On failure, inspect reports/failure_report.json and rerun after correction. Production incremental loads should use transactional storage and a durable run ledger; Parquet overwrite is a local assessment simplification.

## Tests
`python -m pytest -v`
Tests cover reproducible generation, validation/reject reconciliation, empty input, and baseline/optimized equality. Add malformed schema, duplicate dimension key, retry, and late-event tests if extending the implementation.

## Limitations
Local single-machine Spark; no streaming, currency conversion, or transactional incremental upsert. For production, consider Delta/Iceberg/Hudi, schema registry, cluster tuning, and external metrics.
