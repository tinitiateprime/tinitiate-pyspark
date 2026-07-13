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

Each scenario directory contains an independent Python generator named `generate_source.py`. The complete generate → MinIO → PostgreSQL student exercise is in [MinIO source files to PostgreSQL](../MINIO_TO_POSTGRES_SCENARIOS.md).

Use Python to generate a scenario locally:

```cmd
python pyspark-database/scenarios/01_many_small_json_customer/generate_source.py
```

Use the main PySpark loader to load files from MinIO into PostgreSQL:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-database/scripts/postgres.py
```

| # | Detailed lesson | Python generator | Source → target |
| --- | --- | --- | --- |
| 01 | [Many small JSON files to customer](01_many_small_json_customer/README.md) | [`generate_source.py`](01_many_small_json_customer/generate_source.py) | JSON → `customer` |
| 02 | [Many small JSON files to multiple tables](02_many_small_json_multiple_tables/README.md) | [`generate_source.py`](02_many_small_json_multiple_tables/generate_source.py) | JSON → `location`, `product`, `customer`, `sales` |
| 03 | [Many large JSON files to sales](03_many_large_json_sales/README.md) | [`generate_source.py`](03_many_large_json_sales/generate_source.py) | JSON → `sales` |
| 04 | [Many small CSV files to employee](04_many_small_csv_emp/README.md) | [`generate_source.py`](04_many_small_csv_emp/generate_source.py) | CSV → `emp` |
| 05 | [Many small CSV files to multiple tables](05_many_small_csv_multiple_tables/README.md) | [`generate_source.py`](05_many_small_csv_multiple_tables/generate_source.py) | CSV → `dept`, `projects`, `emp`, `emp_projects` |
| 06 | [Many large CSV files to employee](06_many_large_csv_emp/README.md) | [`generate_source.py`](06_many_large_csv_emp/generate_source.py) | CSV → `emp` |
| 07 | [Many small Parquet files to transactions](07_many_small_parquet_transaction/README.md) | [`generate_source.py`](07_many_small_parquet_transaction/generate_source.py) | Parquet → `sales_transaction` |
| 08 | [Many small Parquet files to multiple tables](08_many_small_parquet_multiple_tables/README.md) | [`generate_source.py`](08_many_small_parquet_multiple_tables/generate_source.py) | Parquet → four tables |
| 09 | [Many large Parquet files to sales](09_many_large_parquet_sales/README.md) | [`generate_source.py`](09_many_large_parquet_sales/generate_source.py) | Parquet → `sales` |
| 10 | [Ultra volume up to one million files](10_ultra_one_million_files/README.md) | [`generate_source.py`](10_ultra_one_million_files/generate_source.py) | CSV/JSON/Parquet → `sales_transaction` |
| 11 | [Millions of updates and deletes](11_millions_updates_deletes/README.md) | [`generate_source.py`](11_millions_updates_deletes/generate_source.py) | CDC files → staging → `sales_transaction` |

## Before starting

Complete the PostgreSQL and Python setup in the [main database tutorial](../README.md). Start with Scenario 01, then Scenario 04, Scenario 07, and Scenario 11 to compare formats and understand the complete load lifecycle.

The one-million-file and million-change configurations are opt-in. Run their default scaled versions first.
