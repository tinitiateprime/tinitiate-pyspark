# Scenario 03: Many Large JSON Files to the Sales Table

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Measure Spark parsing and PostgreSQL JDBC throughput when each JSON file contains many sales records.

## Dataset

| Property | Default |
| --- | --- |
| Generated folder | `data/database_scenarios/03_many_large_json_sales/03_json_large_sales/sales` |
| Files | 2 |
| Rows per file | 100,000 |
| Expected rows | 200,000 |
| Target | `training.sales` |
| JDBC writers | 8 |
| JDBC batch size | 20,000 |

Columns include sale, customer, product, and location keys plus quantity, price, and timestamp.

## Run the complete scenario

```powershell
./pyspark-database/scenarios/03_many_large_json_sales/run.ps1
```

```powershell
./pyspark-database/scenarios/03_many_large_json_sales/run.ps1 -FileCount 10 -RowsPerFile 500000 -WritePartitions 8
```

## Phase 1: Check capacity

Confirm free disk, PostgreSQL space, and available memory. Generated JSON is larger than an equivalent Parquet dataset and must be parsed as text.

## Phase 2: Generate large files

The generator streams deterministic records into each newline-delimited JSON part. “Large” is a lab setting; measure actual bytes because row width changes file size.

## Phase 3: Inspect expected volume

Use the manifest as the ingestion contract. Check file sizes as well as counts. A truncated large file may still exist and must be detected by row reconciliation or checksums.

## Phase 4: Load sales

Spark reads all parts together, validates required columns, caches the validated DataFrame for reconciliation, and writes with eight bounded JDBC partitions. Source partitions and JDBC partitions are intentionally separate decisions.

## Phase 5: Validate and benchmark

Defaults should produce 200,000 accepted rows and zero rejects. Compare:

- JSON discovery and parse time (`read_seconds`)
- PostgreSQL batch insert time (`write_seconds`)
- CPU and disk use in Docker Desktop
- Effect of 4, 8, and 16 writers

Stop increasing writers when PostgreSQL contention grows or throughput stops improving.

## Phase 6: Review and cleanup

Compare the same row count as many small JSON files. The format and data are constant; only file layout changes. Keep the generated files until the comparison is complete.

## Best practices demonstrated

- Use newline-delimited JSON for parallel Spark ingestion.
- Reconcile large extracts with manifests and source checksums.
- Tune JDBC batches and connections by measurement.
- Separate source-read bottlenecks from database-write bottlenecks.
