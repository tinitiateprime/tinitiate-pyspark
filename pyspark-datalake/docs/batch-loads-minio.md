# Retail Banking Batch Loads with MinIO

[Back to main README](../../README.md#data-lake-operations) | [Back to Batch Loads Topic](../03_batch_loads/README.md)

This tutorial creates a finance retail-banking dataset and loads it into a MinIO-backed data lake with PySpark.

MinIO is S3-compatible object storage. Spark still uses `s3a://` paths, but the endpoint points to MinIO instead of AWS S3.

## Folder Guide

| Folder | Purpose |
|---|---|
| [01_dataset_design](../01_dataset_design/README.md) | Retail banking tables, columns, and source layout |
| [02_minio_setup](../02_minio_setup/README.md) | MinIO endpoint, bucket, and Spark S3A settings |
| [03_batch_loads](../03_batch_loads/README.md) | Bulk, incremental, and selected table loads |
| [04_compaction](../04_compaction/README.md) | Small-file problem and compaction workflow |
| [05_validation](../05_validation/README.md) | Source checks, MinIO checks, and PySpark validation |

## Dataset

The dataset has master and transactional data for retail banking.

| Area | Tables |
|---|---|
| Customer master | `customers` |
| Account master | `account_plans`, `accounts` |
| Salary deposits | `salary_deposits` |
| Loan master | `loan_types`, `loans` |
| Loan operations | `loan_payments`, `payment_reminders` |
| Account activity | `account_transactions` |

Source files are written as daily batches:

```text
pyspark-datalake/data/source/retail_banking/<table>/batch_date=YYYY-MM-DD/
```

## Step 1: Generate Source Batches

Generate CSV source files:

```powershell
python pyspark-datalake/scripts/generate_retail_banking_dataset.py `
  --output-dir pyspark-datalake/data/source/retail_banking `
  --format csv `
  --overwrite `
  --customers 1000 `
  --days 7 `
  --start-date 2026-01-01 `
  --small-files 4
```

For JSON source files:

```powershell
python pyspark-datalake/scripts/generate_retail_banking_dataset.py `
  --output-dir pyspark-datalake/data/source/retail_banking_json `
  --format json `
  --overwrite `
  --customers 1000 `
  --days 7 `
  --start-date 2026-01-01 `
  --small-files 4
```

Teaching point:

* Each table is delivered in date-based batches.
* `--small-files 4` simulates source systems sending multiple files per table per day.

Inspect the generated source files:

```powershell
python pyspark-datalake/scripts/inspect_retail_banking_batches.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --format csv
```

Teaching point:

* Students should see multiple files for each table and batch date.
* This is the raw source layout before MinIO loading or compaction.

## Step 2: MinIO Settings

Use these environment variables for local MinIO defaults:

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "minioadmin"
$env:MINIO_SECRET_KEY = "minioadmin"
$env:MINIO_BUCKET = "datalake"
```

Create the bucket in MinIO before running the load. You can create it in the MinIO console or with the MinIO client:

```powershell
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/datalake
```

## Step 3: Bulk Load to Bronze

Bulk load all available source batches into the bronze zone.

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  pyspark-datalake/scripts/batch_load_to_minio.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --source-format csv `
  --target-format parquet `
  --load-type bulk `
  --zone bronze `
  --bucket datalake `
  --mode overwrite
```

Target layout in MinIO:

```text
s3a://datalake/retail_banking/bronze/<table>/batch_date=YYYY-MM-DD/
```

Teaching point:

* Bronze stores source-aligned data.
* The target is Parquet even when the source is CSV.
* Data is partitioned by `batch_date`.

## Step 4: Incremental Daily Load

Load only one batch date:

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  pyspark-datalake/scripts/batch_load_to_minio.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --source-format csv `
  --target-format parquet `
  --load-type incremental `
  --batch-date 2026-01-03 `
  --zone bronze `
  --bucket datalake `
  --mode append
```

Teaching point:

* Incremental loads process one date or one arrival window.
* This is the normal daily batch pattern.
* Use `append` when adding a new batch and `overwrite` when reprocessing a batch.

## Step 5: Compact Source Files Before Processing

If the source sends many small files, compact them into fewer files before downstream processing.

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  pyspark-datalake/scripts/batch_load_to_minio.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --source-format csv `
  --target-format parquet `
  --load-type compact `
  --batch-date 2026-01-03 `
  --zone bronze `
  --bucket datalake `
  --output-partitions 2 `
  --mode overwrite
```

Compacted target layout:

```text
s3a://datalake/retail_banking/bronze_compacted/<table>/batch_date=2026-01-03/
```

Teaching point:

* Raw source can arrive as many small files.
* Compaction writes fewer Parquet files.
* Downstream jobs should process compacted files when the raw landing area has too many tiny files.

## Step 6: Load Selected Tables

To teach one scenario at a time, load selected tables only:

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  pyspark-datalake/scripts/batch_load_to_minio.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --source-format csv `
  --target-format parquet `
  --load-type incremental `
  --batch-date 2026-01-03 `
  --tables customers accounts salary_deposits `
  --zone bronze `
  --bucket datalake `
  --mode append
```

## Step 7: What Students Should Compare

Ask students to compare:

| Question | What To Observe |
|---|---|
| How many source files arrived? | Count files under each `batch_date` folder. |
| What format was received? | CSV or JSON source files. |
| What format was written? | Parquet target files in MinIO. |
| Was the load bulk or incremental? | Full source root vs one batch date. |
| Did compaction help? | Fewer output files and faster repeated reads. |

## Notes

* If your Spark distribution already includes the S3A connector, you may not need `--packages`.
* If your Hadoop version is not compatible with `hadoop-aws:3.3.4`, use the matching `hadoop-aws` version for your Spark environment.
* The term `s3a://` is the Spark connector path. The storage service is MinIO because `spark.hadoop.fs.s3a.endpoint` points to the MinIO endpoint.

[Back to main README](../../README.md#data-lake-operations) | [Back to Batch Loads Topic](../03_batch_loads/README.md)
