# Scenario 09: Many Large Parquet Files to the Sales Table

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Benchmark a production-style Parquet scan followed by batched PostgreSQL inserts into `training.sales`.

## Dataset

| Property | Default |
| --- | --- |
| Generated folder | `data/database_scenarios/09_many_large_parquet_sales/09_parquet_large_sales/sales` |
| Files | 2 |
| Rows per file | 100,000 |
| Expected rows | 200,000 |
| Target | `training.sales` |
| JDBC writers | 8 |
| Batch size | 20,000 |

## Run the complete scenario

```powershell
./pyspark-database/scenarios/09_many_large_parquet_sales/run.ps1
```

## Phase 1: Check capacity

Verify PostgreSQL storage and connection capacity. Parquet may make the source scan fast enough that the database becomes the obvious bottleneck.

## Phase 2: Generate large Parquet files

The generator writes Snappy-compressed parts. Row count is configurable; record actual compressed bytes because width and compression ratio determine scan work.

## Phase 3: Inspect metadata

Review file count, rows, schema, compressed size, and row-group statistics. A manifest proves what was expected; it does not replace validation of what arrived.

## Phase 4: Load sales

Spark reads the columnar source and converts rows into JDBC batches. PostgreSQL cannot consume Parquet directly through this path, so fast predicate pushdown does not eliminate row-oriented database insertion cost.

## Phase 5: Validate and compare

Defaults produce 200,000 rows. Compare this result with Scenario 03 using the same rows in JSON. Separate:

- File scan and decode time
- Spark validation time
- JDBC serialization/network time
- PostgreSQL insert/index time

## Phase 6: Review and cleanup

Tune file size and JDBC concurrency independently. For very large one-time PostgreSQL loads, benchmark a staged `COPY` workflow against JDBC while retaining the same validation and audit controls.

## Best practices demonstrated

- Use Parquet for efficient upstream analytics and validation.
- Do not confuse source-format speed with database-write speed.
- Measure index maintenance during bulk insertion.
- Consider `COPY` when maximum PostgreSQL bulk throughput is the priority.
