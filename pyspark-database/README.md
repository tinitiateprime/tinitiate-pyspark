# PySpark Source Files to PostgreSQL Tutorial

This module teaches how to load CSV, newline-delimited JSON, and Parquet source files into PostgreSQL with PySpark. It covers small-file fan-in, large extracts, one-to-many table routing, bounded JDBC writes, rejected records, load auditing, and file-based updates and deletes.

[Back to the main README](../README.md#pyspark-to-postgresql)

## Learning objectives

By the end of the tutorial, you will be able to:

- Read many files with one Spark operation instead of looping over files.
- Route known source folders to one or several PostgreSQL tables.
- Control PostgreSQL connections with Spark write partitions.
- Batch JDBC inserts and separate read time from database write time.
- Quarantine malformed or invalid records before they reach the database.
- Stage millions of changes and apply updates/deletes with set-based SQL.
- Explain why one million source files is primarily a file-discovery problem.

## How to use this course

Start at the [scenario index](scenarios/README.md). Each scenario has two files:

- `README.md` explains the dataset, expected results, architecture, performance questions, and every learning phase.
- `run.ps1` executes those phases in order: prerequisites, generation, inspection, Spark load, PostgreSQL validation, review, and cleanup guidance.
- `generate_source.py` generates that scenario's CSV, JSON, or Parquet files independently.

For the MinIO-based version of every scenario, follow [MinIO source files to PostgreSQL](MINIO_TO_POSTGRES_SCENARIOS.md).

Run the scenarios in this suggested order:

1. Scenario 01 for the basic file-to-table lifecycle.
2. Scenario 02 for explicit multi-table routing and dependencies.
3. Scenarios 03–09 to compare JSON, CSV, and Parquet file layouts.
4. Scenario 10 for file-count stress after scaled tests succeed.
5. Scenario 11 for transactional updates and deletes.

The generated dataset is never a mysterious prerequisite: every scenario creates its own deterministic files under `data/database_scenarios`, prints its manifest, and leaves the files available for inspection.

Use the [data dictionary](DATASETS.md) for every table, column, key, relationship, dataset profile, and reconciliation rule.

## Scenario coverage

| # | Detailed scenario | Target table(s) | Generated folder |
| --- | --- | --- | --- |
| 1 | [Many small JSON files to one table](scenarios/01_many_small_json_customer/README.md) | `customer` | `01_json_small_customer` |
| 2 | [Many small JSON files split across tables](scenarios/02_many_small_json_multiple_tables/README.md) | `customer`, `sales`, `product`, `location` | `02_json_small_multi` |
| 3 | [Many large JSON files to one table](scenarios/03_many_large_json_sales/README.md) | `sales` | `03_json_large_sales` |
| 4 | [Many small CSV files to one table](scenarios/04_many_small_csv_emp/README.md) | `emp` | `04_csv_small_emp` |
| 5 | [Many small CSV files split across tables](scenarios/05_many_small_csv_multiple_tables/README.md) | `emp`, `dept`, `projects`, `emp_projects` | `05_csv_small_multi` |
| 6 | [Many large CSV files to one table](scenarios/06_many_large_csv_emp/README.md) | `emp` | `06_csv_large_emp` |
| 7 | [Many small Parquet files to one table](scenarios/07_many_small_parquet_transaction/README.md) | `sales_transaction` | `07_parquet_small_sales_transaction` |
| 8 | [Many small Parquet files split across tables](scenarios/08_many_small_parquet_multiple_tables/README.md) | `customer`, `sales`, `product`, `location` | `08_parquet_small_multi` |
| 9 | [Many large Parquet files to one table](scenarios/09_many_large_parquet_sales/README.md) | `sales` | `09_parquet_large_sales` |
| 10 | [Up to one million files](scenarios/10_ultra_one_million_files/README.md) | `sales_transaction` | `10_ultra_sales_transaction` |
| 11 | [File-based updates and deletes](scenarios/11_millions_updates_deletes/README.md) | `sales_transaction` | `11_cdc_sales_transaction` |

## Module structure

```text
pyspark-database/
├── ti-data-engineering-docker-compose.yml  # Full Docker stack: PostgreSQL, MinIO, Spark, Jupyter, Kafka, Airflow
├── docker-compose.yml
├── DATASETS.md                  # Table schemas, relationships, and data profiles
├── MINIO_TO_POSTGRES_SCENARIOS.md
├── postgres.env.example
├── requirements.txt
├── scenarios/                  # One detailed lesson and run script per scenario
├── sql/
│   └── 01_schema.sql
└── scripts/
    ├── apply_sales_transaction_cdc.py
    ├── generate_database_sources.py
    ├── publish_minio_lab.py
    ├── upload_sources_to_minio.py
    └── load_files_to_postgres.py
```

Generated source data is written under `data/` and is ignored by Git.

## 1. Start PostgreSQL and MinIO

For the MinIO-to-PostgreSQL lab, use the full data engineering Docker stack:

```cmd
docker compose -f pyspark-database/ti-data-engineering-docker-compose.yml up -d
docker ps
```

This starts:

- PostgreSQL container `postgres` on `localhost:5432`
- MinIO container `minio` on `localhost:9000`
- MinIO browser console on `http://localhost:9001`
- Jupyter on `http://localhost:8888`
- Spark master and worker services

The compose file starts PostgreSQL. Apply [`01_schema.sql`](sql/01_schema.sql) after the container is running to create the `training` schema, target tables, CDC staging table, audit table, and indexes.

If your instructor already has `postgres` and `minio` running, keep using those containers and do not start duplicate containers.

Apply and check the schema:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -v ON_ERROR_STOP=1 -U ti_dbuser -d tinitiateai -f /lab/sql/01_schema.sql
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "\dt training.*"
```

The Docker compose file mounts [`01_schema.sql`](sql/01_schema.sql) into the PostgreSQL container at `/lab/sql/01_schema.sql`. The command above runs the DDL inside Docker using these lab database credentials:

- Database: `tinitiateai`
- User: `ti_dbuser`
- Password: `tiuser!23456`

## 2. Prepare Python and JDBC

```cmd
python -m pip install --user -r pyspark-database/requirements.txt
```

For MinIO publishing only, this smaller install is enough:

```cmd
python -m pip install --user minio pyarrow
```

The examples use the PostgreSQL JDBC driver package:

```text
org.postgresql:postgresql:42.7.4
```

Set credentials in the shell:

```cmd
set POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/tinitiateai
set POSTGRES_USER=ti_dbuser
set POSTGRES_PASSWORD=tiuser!23456

set MINIO_ENDPOINT=http://localhost:9000
set MINIO_ACCESS_KEY=minio
set MINIO_SECRET_KEY=minio123
set MINIO_BUCKET=datalake
```

When Spark runs inside the Docker/Jupyter container, use container names instead of `localhost`:

```cmd
set POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/tinitiateai
set MINIO_ENDPOINT=http://minio:9000
```

For the full MinIO workflow, including bucket paths and scenario commands, use [MinIO source files to PostgreSQL](MINIO_TO_POSTGRES_SCENARIOS.md).

## 3. Generate the source scenarios

### Laptop-sized tutorial data

```powershell
python pyspark-database/scripts/generate_database_sources.py `
  --output-dir data/database_sources `
  --scenario all `
  --small-file-count 20 `
  --small-rows-per-file 10 `
  --large-file-count 2 `
  --large-rows-per-file 100000 `
  --overwrite
```

This creates scenarios 1–9 plus update/delete files in CSV, JSON, and Parquet. The ultra-volume scenario is excluded from `all` and must be requested explicitly.

Each manifest under `data/database_sources/_manifests` records the expected tables, formats, file counts, and row counts.

## 4. Reusable load command

All scenarios use [`load_files_to_postgres.py`](scripts/load_files_to_postgres.py):

```powershell
spark-submit `
  --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path <source-folder> `
  --source-format <csv|json|parquet> `
  --target-table <table> `
  --scenario <scenario-name> `
  --write-mode overwrite `
  --write-partitions 4 `
  --batch-size 10000
```

The loader:

1. Reads an entire folder in one Spark operation.
2. Applies an explicit table contract and type conversions.
3. Separates valid and rejected rows.
4. Limits concurrent PostgreSQL writers with `--write-partitions`.
5. Uses JDBC batches instead of one insert per row.
6. Writes load timing and counts to `training.load_audit`.

`--write-mode overwrite` truncates the training target before a repeatable lab run. Use `append` for genuinely new keys.

## 5. Quick-reference JSON commands

### Many small JSON files to `customer`

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_sources/01_json_small_customer/customer `
  --source-format json --target-table customer `
  --scenario json-small-customer --write-mode overwrite `
  --write-partitions 4 --batch-size 10000 --expected-files 20
```

Do not launch one Spark job or JDBC connection per JSON file. Spark should perform the file fan-in, then write a bounded number of database partitions.

### Many small JSON files split into four tables

Load independent dimensions first, then dependent facts:

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/02_json_small_multi/location --source-format json --target-table location --scenario json-small-multi --write-mode overwrite --write-partitions 2 --expected-files 20
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/02_json_small_multi/product --source-format json --target-table product --scenario json-small-multi --write-mode overwrite --write-partitions 2 --expected-files 20
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/02_json_small_multi/customer --source-format json --target-table customer --scenario json-small-multi --write-mode overwrite --write-partitions 2 --expected-files 20
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/02_json_small_multi/sales --source-format json --target-table sales --scenario json-small-multi --write-mode overwrite --write-partitions 4 --expected-files 20
```

Use an explicit source-to-target mapping. Deriving arbitrary database table names from incoming filenames is unsafe and makes schema governance difficult.

### Many large JSON files to `sales`

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_sources/03_json_large_sales/sales `
  --source-format json --target-table sales `
  --scenario json-large-sales --write-mode overwrite `
  --write-partitions 8 --batch-size 20000 --expected-files 2
```

Large files usually reduce file-open overhead, but JDBC throughput is controlled by accepted rows, row width, PostgreSQL resources, batch size, and concurrent connections—not the source file count alone.

## 6. Quick-reference CSV commands

### Many small CSV files to `emp`

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_sources/04_csv_small_emp/emp `
  --source-format csv --target-table emp `
  --scenario csv-small-emp --write-mode overwrite `
  --write-partitions 4 --batch-size 10000 --expected-files 20
```

The loader supplies a schema, captures malformed rows, and casts dates and numbers before writing.

### Many small CSV files split into four tables

Recommended order: `dept`, `projects`, `emp`, then `emp_projects`.

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/05_csv_small_multi/dept --source-format csv --target-table dept --scenario csv-small-multi --write-mode overwrite --write-partitions 2 --expected-files 20
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/05_csv_small_multi/projects --source-format csv --target-table projects --scenario csv-small-multi --write-mode overwrite --write-partitions 2 --expected-files 20
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/05_csv_small_multi/emp --source-format csv --target-table emp --scenario csv-small-multi --write-mode overwrite --write-partitions 4 --expected-files 20
spark-submit --packages org.postgresql:postgresql:42.7.4 pyspark-database/scripts/load_files_to_postgres.py --source-path data/database_sources/05_csv_small_multi/emp_projects --source-format csv --target-table emp_projects --scenario csv-small-multi --write-mode overwrite --write-partitions 4 --expected-files 20
```

### Many large CSV files to `emp`

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_sources/06_csv_large_emp/emp `
  --source-format csv --target-table emp `
  --scenario csv-large-emp --write-mode overwrite `
  --write-partitions 8 --batch-size 20000 --expected-files 2
```

## 7. Quick-reference Parquet commands

### Many small Parquet files to `sales_transaction`

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_sources/07_parquet_small_sales_transaction/sales_transaction `
  --source-format parquet --target-table sales_transaction `
  --scenario parquet-small-transaction --write-mode overwrite `
  --write-partitions 4 --batch-size 10000 --expected-files 20
```

### Many small Parquet files split into four tables

Use the same `location`, `product`, `customer`, `sales` order shown for multi-table JSON, changing the folder to `08_parquet_small_multi` and `--source-format` to `parquet`.

### Many large Parquet files to `sales`

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_sources/09_parquet_large_sales/sales `
  --source-format parquet --target-table sales `
  --scenario parquet-large-sales --write-mode overwrite `
  --write-partitions 8 --batch-size 20000 --expected-files 2
```

Parquet improves Spark-side scans through columnar storage and metadata. PostgreSQL still receives rows through JDBC, so source scan speed and database insert speed must be measured separately.

## 8. Quick-reference ultra-volume commands

Start with 10,000 one-row files:

```powershell
python pyspark-database/scripts/generate_database_sources.py `
  --output-dir data/database_ultra_10000 `
  --scenario ultra --ultra-format json `
  --ultra-file-count 10000 --ultra-rows-per-file 1 `
  --allow-ultra --overwrite
```

The full one-million-file command is intentionally explicit:

```powershell
python pyspark-database/scripts/generate_database_sources.py `
  --output-dir data/database_ultra_1000000 `
  --scenario ultra --ultra-format json `
  --ultra-file-count 1000000 --ultra-rows-per-file 1 `
  --allow-ultra --overwrite
```

> [!WARNING]
> One million local files can take hours to create, list, scan, and delete. It can exhaust inodes or destabilize notebooks and IDEs. Prefer an isolated filesystem or object-store prefix with lifecycle cleanup.

Load the scaled experiment:

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/load_files_to_postgres.py `
  --source-path data/database_ultra_10000/10_ultra_sales_transaction/sales_transaction `
  --source-format json --target-table sales_transaction `
  --scenario ultra-10000 --write-mode overwrite `
  --write-partitions 4 --batch-size 10000 --expected-files 10000
```

The best production fix is upstream compaction. If that is impossible, process stable prefixes incrementally, persist a manifest/checkpoint, compact to Parquet, and load the database from the compacted data. Increasing JDBC connections does not solve source-file listing overhead.

## 9. Quick-reference update/delete commands

CSV, JSON, and Parquet files do not update PostgreSQL rows by themselves. The safe pattern is:

1. Read and validate the change files with Spark.
2. Keep the latest change per transaction key and write it to PostgreSQL staging with bounded JDBC writers.
3. Invoke PostgreSQL's `training.apply_sales_transaction_changes()` function to apply set-based `UPDATE` and `DELETE` statements atomically.
4. Reconcile staged, rejected, updated, deleted, and missing target keys.

Run the standard detailed exercise with a 200,000-row base, 100,000 updates, and 10,000 deletes:

```powershell
./pyspark-database/scenarios/11_millions_updates_deletes/run.ps1
```

Expected result: 100,000 rows updated, 10,000 rows deleted, 0 unmatched changes, and 190,000 rows remaining.

First load a base `sales_transaction` dataset. Then apply the default CDC files:

```powershell
spark-submit --packages org.postgresql:postgresql:42.7.4 `
  pyspark-database/scripts/apply_sales_transaction_cdc.py `
  --source-path data/database_sources/11_cdc_sales_transaction/parquet/sales_transaction_changes `
  --source-format parquet `
  --write-partitions 4 --batch-size 10000
```

Generate a larger, practical millions-of-changes exercise without creating millions of source files:

```powershell
python pyspark-database/scripts/generate_database_sources.py `
  --output-dir data/database_cdc_millions `
  --scenario ultra cdc `
  --ultra-format parquet --ultra-file-count 60 --ultra-rows-per-file 100000 `
  --cdc-updates 5000000 --cdc-deletes 1000000 --cdc-rows-per-file 100000 `
  --allow-ultra --overwrite
```

Load the six-million-row base, then apply the Parquet change files. The CDC script reports accepted/rejected changes and the number of target rows actually updated/deleted.

Never issue one database statement per change row. Millions of network round trips, commits, and index probes will overwhelm both Spark and PostgreSQL.

## 10. Validate the results

```powershell
docker exec -e PGPASSWORD='tiuser!23456' postgres psql -U ti_dbuser -d tinitiateai -c "SELECT COUNT(*) FROM training.customer;"
docker exec -e PGPASSWORD='tiuser!23456' postgres psql -U ti_dbuser -d tinitiateai -c "SELECT * FROM training.load_audit ORDER BY load_id DESC LIMIT 20;"
docker exec -e PGPASSWORD='tiuser!23456' postgres psql -U ti_dbuser -d tinitiateai -c "SELECT COUNT(*) FROM training.sales_transaction;"
```

After a large load or major update/delete exercise:

```sql
ANALYZE training.sales;
ANALYZE training.sales_transaction;
```

Use `VACUUM (ANALYZE)` after large deletes when appropriate; do not run `VACUUM FULL` casually because it takes an exclusive lock and rewrites the table.

## 11. Best-practice checklist

### Source files

- Require stable files, manifests, checksums, and replayable batch IDs.
- Use explicit schemas; never rely on inference for recurring production loads.
- Read a directory/prefix once instead of looping over files in Python.
- Compact tiny files before repeated downstream processing.
- Preserve source filename and rejection reason for failed records.
- Keep accepted, rejected, missing, and duplicate counts reconcilable.

### Spark

- Separate file discovery/read metrics from database write metrics.
- Avoid unnecessary `count()` actions in production; this tutorial counts to teach reconciliation.
- Repartition by desired JDBC concurrency, not by source file count.
- Watch driver memory during very large file listings.
- Use the Spark UI to measure scheduling delay, tasks, input bytes, skew, and retries.

### PostgreSQL

- Keep `--write-partitions` comfortably below available PostgreSQL connections.
- Start with 4–8 writers and a batch size around 5,000–20,000, then measure.
- Load independent tables concurrently only when PostgreSQL has capacity.
- Use staging plus set-based SQL for updates, deletes, deduplication, and idempotency.
- Enforce primary keys and index join/filter columns, but account for index maintenance during bulk loads.
- For maximum one-time bulk throughput, benchmark PostgreSQL `COPY` against JDBC.
- Use TLS and a secret manager outside local training; never commit real passwords.

## 12. Failure and restart design

A production load should have a batch ID and state transitions such as `discovered`, `validated`, `staged`, `committed`, or `failed`. Do not mark source files complete until the database transaction and audit reconciliation succeed.

For restartability:

- Make source batches immutable.
- Reject duplicate batch IDs or make replay behavior explicit.
- Load to staging first when target keys may already exist.
- Commit target changes and audit state together when possible.
- Keep dead-letter records separate from files waiting for infrastructure retry.

[Back to the main README](../README.md#pyspark-to-postgresql)

