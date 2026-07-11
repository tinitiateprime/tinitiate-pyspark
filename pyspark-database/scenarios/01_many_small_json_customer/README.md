# Scenario 01: Many Small JSON Files to One Customer Table

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Read many newline-delimited JSON files as one Spark DataFrame and load the accepted rows into `training.customer` with a controlled number of PostgreSQL connections.

## Dataset

| Property | Default |
| --- | --- |
| Source format | Newline-delimited JSON |
| Generated folder | `data/database_scenarios/01_many_small_json_customer/01_json_small_customer/customer` |
| Files | 20 |
| Rows per file | 10 |
| Expected rows | 200 |
| Target | `training.customer` |
| Primary key | `customer_id` |

Columns: `customer_id`, `customer_name`, `email`, `location_id`, and `created_at`.

## Run the complete scenario

```powershell
./pyspark-database/scenarios/01_many_small_json_customer/run.ps1
```

Scale the experiment without editing code:

```powershell
./pyspark-database/scenarios/01_many_small_json_customer/run.ps1 -FileCount 1000 -RowsPerFile 1
```

## Phase 1: Check prerequisites

The script verifies that Python and Docker are available and that `ti-batch-postgres` is running on the training Docker stack.

## Phase 2: Generate the dataset

`generate_database_sources.py` creates deterministic customer IDs and writes the requested number of JSON files. It also writes `_manifests/json-small-customer.json` with expected file and row counts.

## Phase 3: Inspect before loading

Confirm that the manifest says `files × rows_per_file = rows`. Open several parts and verify that each physical line contains one JSON object. A multi-line JSON document is a different ingestion pattern.

## Phase 4: Load PostgreSQL

The scenario calls `load_files_to_postgres.py` once for the whole directory. The loader applies the customer contract, casts timestamps, quarantines invalid rows, repartitions accepted data into four JDBC writers, and inserts in batches.

Do not loop over files and open one database connection per file. File fan-in belongs in Spark; JDBC concurrency should be based on database capacity.

## Phase 5: Validate

The script checks the target count and the latest `training.load_audit` row. Expected defaults:

- Accepted: 200
- Rejected: 0
- Target rows: 200
- Source files: 20

Compare `read_seconds` with `write_seconds`. With tiny inputs, startup and file-open overhead may dominate.

## Phase 6: Review and cleanup

Repeat with 20, 100, and 1,000 files while keeping total rows constant. This isolates file-count overhead from data volume. The script prints the exact generated folder and cleanup command but leaves data in place for inspection.

## Best practices demonstrated

- Read the directory once with an explicit schema.
- Preserve source file and rejected payload metadata.
- Bound JDBC partitions independently of source file count.
- Reconcile expected, accepted, rejected, and target rows.
