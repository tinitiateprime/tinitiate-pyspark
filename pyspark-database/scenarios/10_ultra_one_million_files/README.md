# Scenario 10: Ultra Volume—Up to One Million Files

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Demonstrate that extreme file counts are a discovery, metadata, scheduling, and cleanup problem before they become a PostgreSQL loading problem.

## Dataset

| Property | Safe default | Full stress test |
| --- | ---: | ---: |
| Files | 10,000 | 1,000,000 |
| Rows per file | 1 | 1 |
| Format | JSON | Configurable |
| Target | `training.sales_transaction` | `training.sales_transaction` |

Generated folder: `data/database_scenarios/10_ultra_one_million_files/10_ultra_sales_transaction/sales_transaction`.

## Run the scaled scenario first

```powershell
./pyspark-database/scenarios/10_ultra_one_million_files/run.ps1
```

Only after validating disk, inode, runtime, and cleanup capacity:

```powershell
./pyspark-database/scenarios/10_ultra_one_million_files/run.ps1 `
  -FileCount 1000000 -RowsPerFile 1 -Format json -ConfirmMillionFiles
```

## Phase 1: Capacity and safety

The script requires explicit confirmation for one million files. Use an isolated data directory or object-store prefix with a tested lifecycle policy. File creation and deletion may each take hours.

## Phase 2: Generate files

The generator reports progress every 10,000 files. The manifest is written outside table directories so Spark does not parse it as source data.

## Phase 3: Inspect discovery cost

Measure generation time, directory listing time, Spark planning delay, and driver memory before measuring data processing. With one row per file, metadata massively outweighs payload.

## Phase 4: Load PostgreSQL

Spark reads the prefix as one dataset and writes through only four JDBC partitions. One million files must never create one million database connections.

## Phase 5: Validate and decompose timing

Reconcile expected files, accepted rows, rejected rows, and target rows. Separate:

- Object/filesystem listing
- Spark scan planning
- Task scheduling
- Parsing/decoding
- JDBC and PostgreSQL writing

## Phase 6: Compaction and cleanup

The production answer is usually upstream compaction, not larger Spark drivers or more JDBC writers. Process stable prefixes incrementally, checkpoint a manifest, compact to reasonably sized Parquet, then load PostgreSQL.

The script leaves files for analysis and prints a cleanup command. Do not delete a computed path without verifying it points inside the intended data directory.

## Best practices demonstrated

- Safety-gate destructive or resource-intensive labs.
- Never map source file count to database connection count.
- Make file discovery observable.
- Compact before repeated processing and database loading.
