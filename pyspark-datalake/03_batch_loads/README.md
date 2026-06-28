# 03 Batch Loads

[Back to main README](../../README.md#data-lake-operations)

This folder explains the retail banking batch-load patterns.

## Load Types

| Load Type | Meaning | Typical Mode |
|---|---|---|
| Bulk | Load all source dates for all selected tables | `overwrite` |
| Incremental | Load one batch date | `append` |
| Reprocess | Reload one failed or corrected batch date | `overwrite` |
| Compact | Convert many small files into fewer Parquet files | `overwrite` |

## Script

Main script:

```text
pyspark-datalake/scripts/batch_load_to_minio.py
```

It can:

* read CSV, JSON, or Parquet source files
* write Parquet, CSV, or JSON target files
* write to MinIO using `s3a://`
* load all tables or selected tables
* run bulk, incremental, or compact loads
* partition the target by `batch_date`

## Bulk Load

Use bulk load for an initial load or classroom reset.

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

Target:

```text
s3a://datalake/retail_banking/bronze/<table>/batch_date=YYYY-MM-DD/
```

## Incremental Load

Use incremental load for one daily batch.

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

## Selected Table Load

Use selected tables when teaching one business process.

Salary banking example:

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

Loan operations example:

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  pyspark-datalake/scripts/batch_load_to_minio.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --source-format csv `
  --target-format parquet `
  --load-type incremental `
  --batch-date 2026-01-03 `
  --tables customers loans loan_payments payment_reminders `
  --zone bronze `
  --bucket datalake `
  --mode append
```

## Teaching Notes

Explain these ideas while running the load:

* Source files may be CSV or JSON.
* Data lake targets are commonly Parquet for analytics.
* Bronze keeps source-aligned history.
* `batch_date` helps incremental loading and partition pruning.
* `append` adds new data.
* `overwrite` is used for resets or reprocessing.

[Back to main README](../../README.md#data-lake-operations)
