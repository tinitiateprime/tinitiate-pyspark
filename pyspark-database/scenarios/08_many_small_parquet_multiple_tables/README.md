# Scenario 08: Many Small Parquet Files to Multiple Tables

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Route Parquet folders into customer, product, location, and sales targets while preserving per-table contracts, audit records, and dependencies.

## Dataset

| Table | Default files | Rows/file | Expected rows |
| --- | ---: | ---: | ---: |
| `location` | 20 | 10 | 200 |
| `product` | 20 | 10 | 200 |
| `customer` | 20 | 10 | 200 |
| `sales` | 20 | 10 | 200 |

Generated root: `data/database_scenarios/08_many_small_parquet_multiple_tables/08_parquet_small_multi`.

## Run the complete scenario

```powershell
./pyspark-database/scenarios/08_many_small_parquet_multiple_tables/run.ps1
```

## Phase 1: Check prerequisites

Docker, Python, and the tutorial PostgreSQL container must be available.

## Phase 2: Generate four Parquet datasets

Each table gets its own directory and physical schema. The folder boundary is the routing contract.

## Phase 3: Inspect manifest and schemas

Reconcile each table independently and inspect Parquet metadata. Detect incompatible schema evolution before combining parts into one DataFrame.

## Phase 4: Load in dependency order

The script loads `location`, `product`, `customer`, then `sales`. Spark may process Parquet folders in parallel internally, while JDBC writer counts remain deliberately small.

## Phase 5: Validate

Defaults yield 200 rows per target. Review all audit entries and run relationship checks between sales and its dimensions. Audit timing may differ by table even with identical row counts because row widths differ.

## Phase 6: Review and cleanup

Compact each table independently. Never compact unrelated schemas into the same Parquet directory. Evaluate whether small stable dimensions should be loaded through a simpler path when Spark startup costs exceed the work.

## Best practices demonstrated

- Keep one compatible schema per source directory.
- Route only through a controlled mapping.
- Compact and reconcile per table.
- Choose the simplest reliable tool for genuinely tiny reference data.
