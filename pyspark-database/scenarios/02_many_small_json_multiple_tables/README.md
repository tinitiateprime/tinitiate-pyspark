# Scenario 02: Many Small JSON Files to Multiple Tables

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Route known JSON folders into `training.location`, `training.product`, `training.customer`, and `training.sales`, loading dimensions before the sales fact table.

## Dataset

| Table | Default files | Rows/file | Expected rows | Key |
| --- | ---: | ---: | ---: | --- |
| `location` | 20 | 10 | 200 | `location_id` |
| `product` | 20 | 10 | 200 | `product_id` |
| `customer` | 20 | 10 | 200 | `customer_id` |
| `sales` | 20 | 10 | 200 | `sale_id` |

Generated root: `data/database_scenarios/02_many_small_json_multiple_tables/02_json_small_multi`.

## Run the complete scenario

```powershell
./pyspark-database/scenarios/02_many_small_json_multiple_tables/run.ps1
```

```powershell
./pyspark-database/scenarios/02_many_small_json_multiple_tables/run.ps1 -FilesPerTable 100 -RowsPerFile 100
```

## Phase 1: Check prerequisites

The script confirms Docker, Python, and the isolated PostgreSQL container before creating files.

## Phase 2: Generate four datasets

The generator creates one folder per approved table. The folder-to-target mapping is explicit in `run.ps1`; incoming names never become arbitrary SQL identifiers.

## Phase 3: Inspect the manifest

The manifest contains four entries. Reconcile files and rows separately for every table. Do not rely only on a grand total because one missing dimension file can invalidate fact-table relationships.

## Phase 4: Load in dependency order

The script loads `location`, `product`, `customer`, and finally `sales`. Small dimensions use two JDBC writers; sales uses four. Independent dimensions may run concurrently only when PostgreSQL has enough CPU, I/O, locks, and connections.

Each folder is read into one DataFrame. “Split files into multiple tables” means controlled routing by folder/schema—not splitting arbitrary fields from a mixed record without a data contract.

## Phase 5: Validate each table

The script queries all four counts and displays four audit rows. With defaults, each table should contain 200 rows and have zero rejects.

Also validate relationships:

```sql
SELECT COUNT(*) AS sales_without_product
FROM training.sales s
LEFT JOIN training.product p ON p.product_id = s.product_id
WHERE p.product_id IS NULL;
```

## Phase 6: Review and cleanup

Compare dimension and fact write times. Try two versus four writers for `sales`; do not assume higher concurrency is faster. Generated data remains available until you use the printed cleanup command.

## Best practices demonstrated

- Maintain a whitelist of source-to-target mappings.
- Load dimensions before facts when dependencies exist.
- Audit and reconcile each target independently.
- Tune database concurrency per table size.
