# PySpark-to-PostgreSQL Scenario Index

[Back to main database tutorial](../README.md) | [Data dictionary](../DATASETS.md)

Every scenario follows the same learning phases:

1. Check prerequisites and capacity.
2. Generate a deterministic source dataset.
3. Inspect its manifest, schema, files, and expected rows.
4. Load with PySpark using bounded JDBC writers.
5. Validate PostgreSQL and audit/rejection metrics.
6. Review performance, best practices, and cleanup.

Scenario 11 adds a separate atomic CDC phase and reconciliation phase.

| # | Detailed lesson | Runnable script | Source → target |
| --- | --- | --- | --- |
| 01 | [Many small JSON files to customer](01_many_small_json_customer/README.md) | [`run.ps1`](01_many_small_json_customer/run.ps1) | JSON → `customer` |
| 02 | [Many small JSON files to multiple tables](02_many_small_json_multiple_tables/README.md) | [`run.ps1`](02_many_small_json_multiple_tables/run.ps1) | JSON → `location`, `product`, `customer`, `sales` |
| 03 | [Many large JSON files to sales](03_many_large_json_sales/README.md) | [`run.ps1`](03_many_large_json_sales/run.ps1) | JSON → `sales` |
| 04 | [Many small CSV files to employee](04_many_small_csv_emp/README.md) | [`run.ps1`](04_many_small_csv_emp/run.ps1) | CSV → `emp` |
| 05 | [Many small CSV files to multiple tables](05_many_small_csv_multiple_tables/README.md) | [`run.ps1`](05_many_small_csv_multiple_tables/run.ps1) | CSV → `dept`, `projects`, `emp`, `emp_projects` |
| 06 | [Many large CSV files to employee](06_many_large_csv_emp/README.md) | [`run.ps1`](06_many_large_csv_emp/run.ps1) | CSV → `emp` |
| 07 | [Many small Parquet files to transactions](07_many_small_parquet_transaction/README.md) | [`run.ps1`](07_many_small_parquet_transaction/run.ps1) | Parquet → `sales_transaction` |
| 08 | [Many small Parquet files to multiple tables](08_many_small_parquet_multiple_tables/README.md) | [`run.ps1`](08_many_small_parquet_multiple_tables/run.ps1) | Parquet → four tables |
| 09 | [Many large Parquet files to sales](09_many_large_parquet_sales/README.md) | [`run.ps1`](09_many_large_parquet_sales/run.ps1) | Parquet → `sales` |
| 10 | [Ultra volume up to one million files](10_ultra_one_million_files/README.md) | [`run.ps1`](10_ultra_one_million_files/run.ps1) | CSV/JSON/Parquet → `sales_transaction` |
| 11 | [Millions of updates and deletes](11_millions_updates_deletes/README.md) | [`run.ps1`](11_millions_updates_deletes/run.ps1) | CDC files → staging → `sales_transaction` |

## Before starting

Complete the PostgreSQL and Python setup in the [main database tutorial](../README.md). Start with Scenario 01, then Scenario 04, Scenario 07, and Scenario 11 to compare formats and understand the complete load lifecycle.

The one-million-file and million-change configurations are opt-in. Run their default scaled versions first.
