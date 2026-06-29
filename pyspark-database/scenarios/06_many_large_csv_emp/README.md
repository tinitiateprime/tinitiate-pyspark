# Scenario 06: Many Large CSV Files to the Employee Table

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Benchmark CSV parsing separately from PostgreSQL JDBC insertion when employee extracts contain many rows per file.

## Dataset

| Property | Default |
| --- | --- |
| Generated folder | `data/database_scenarios/06_many_large_csv_emp/06_csv_large_emp/emp` |
| Files | 2 |
| Rows per file | 100,000 |
| Expected rows | 200,000 |
| Target | `training.emp` |
| JDBC writers | 8 |
| Batch size | 20,000 |

## Run the complete scenario

```powershell
./pyspark-database/scenarios/06_many_large_csv_emp/run.ps1
```

## Phase 1: Check capacity

Check host disk, container memory, and PostgreSQL capacity. Large CSV generation and parsing are CPU- and I/O-intensive.

## Phase 2: Generate large CSV files

The generator streams records with one header per file. Use `-FileCount` and `-RowsPerFile` to change physical layout while holding total rows constant.

## Phase 3: Inspect the extract

Reconcile manifest rows, physical file count, file bytes, and headers. In production, add source checksums and immutable batch identifiers.

## Phase 4: Load employees

Spark uses an explicit raw-string schema, casts target values, quarantines errors, and writes with bounded JDBC partitions. Eight partitions mean at most eight concurrent table writers, not one writer per source file.

## Phase 5: Validate and tune

Defaults should produce 200,000 accepted rows. Compare 4, 8, and 16 writers and batch sizes of 5,000, 10,000, and 20,000. Capture:

- Read and parse time
- Database write time
- PostgreSQL CPU, disk, and active connections
- Spark task failures or retries

## Phase 6: Review and cleanup

Compare this scenario with Scenario 04 at the same row volume. Fewer large files reduce metadata overhead, but oversized files may limit parallelism.

## Best practices demonstrated

- Measure bytes and task duration, not only row count.
- Keep explicit schemas and rejection handling at scale.
- Tune JDBC concurrency against database capacity.
- Make batches replayable and idempotent.
