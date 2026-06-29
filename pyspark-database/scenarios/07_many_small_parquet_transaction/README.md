# Scenario 07: Many Small Parquet Files to One Transaction Table

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Show that Parquet's columnar format does not eliminate the small-file problem when loading `training.sales_transaction`.

## Dataset

| Property | Default |
| --- | --- |
| Generated folder | `data/database_scenarios/07_many_small_parquet_transaction/07_parquet_small_sales_transaction/sales_transaction` |
| Files | 20 |
| Rows per file | 10 |
| Expected rows | 200 |
| Target | `training.sales_transaction` |
| Primary key | `transaction_id` |

## Run the complete scenario

```powershell
./pyspark-database/scenarios/07_many_small_parquet_transaction/run.ps1
```

## Phase 1: Check prerequisites

The script verifies the isolated PostgreSQL service before generating Parquet.

## Phase 2: Generate Parquet parts

PyArrow writes Snappy-compressed files with embedded schema. The manifest records expected physical files and rows.

## Phase 3: Inspect metadata

Check file sizes and Parquet metadata. Ten narrow rows make a very small file; footer, listing, opening, and task scheduling can cost more than scanning its values.

## Phase 4: Load transactions

Spark reads Parquet types, applies target casts and required-field validation, then repartitions accepted rows for four JDBC writers. PostgreSQL receives rows; it does not receive Parquet pages.

## Phase 5: Validate

Defaults produce 200 rows and zero rejects. Compare source files, Spark input tasks, and audit timing. Explain why a fast Parquet scan can still be followed by a slower database insert.

## Phase 6: Review and cleanup

Scale to 1,000 one-row files and compare with compacted Parquet. Target compressed file size—often 128–512 MB for analytical storage—is more meaningful than a universal rows-per-file rule.

## Best practices demonstrated

- Parquet still requires compaction when files are tiny.
- File layout affects Spark; JDBC settings affect PostgreSQL.
- Validate schema compatibility before writing.
- Reconcile file, row, reject, and target counts.
