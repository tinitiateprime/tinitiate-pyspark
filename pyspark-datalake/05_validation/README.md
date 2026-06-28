# 05 Validation and Student Checks

[Back to main README](../../README.md#data-lake-operations)

This folder contains checks students can run after generating data and loading to MinIO.

## Check Source File Counts

```powershell
python pyspark-datalake/scripts/inspect_retail_banking_batches.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --format csv
```

What to verify:

* every table has folders for each `batch_date`
* each `batch_date` has multiple files
* this proves the source is a small-file style feed

## Check MinIO Paths

Use the MinIO client:

```powershell
mc ls local/datalake/retail_banking/bronze/
mc ls local/datalake/retail_banking/bronze_compacted/
```

Check one table:

```powershell
mc ls local/datalake/retail_banking/bronze/customers/
mc ls local/datalake/retail_banking/bronze_compacted/customers/
```

## Read a Bronze Table with PySpark

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("validate-minio-load")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

customers = spark.read.parquet("s3a://datalake/retail_banking/bronze/customers")
customers.groupBy("batch_date").count().show()
```

## Simple Banking Checks

After loading the tables, students can answer:

| Question | Tables |
|---|---|
| How many customers were loaded per batch date? | `customers` |
| Which account plans are available? | `account_plans` |
| How many salary deposits arrived per day? | `salary_deposits` |
| How many loan payments failed? | `loan_payments` |
| Which customers received reminders? | `payment_reminders` |
| Which channels have the most transactions? | `account_transactions` |

## Example Queries

Salary deposit count by date:

```python
salary = spark.read.parquet("s3a://datalake/retail_banking/bronze/salary_deposits")
salary.groupBy("batch_date").count().orderBy("batch_date").show()
```

Failed loan payments:

```python
payments = spark.read.parquet("s3a://datalake/retail_banking/bronze/loan_payments")
payments.where("payment_status = 'failed'").groupBy("batch_date").count().show()
```

Transaction channel summary:

```python
txns = spark.read.parquet("s3a://datalake/retail_banking/bronze/account_transactions")
txns.groupBy("channel").count().orderBy("count", ascending=False).show()
```

## Completion Checklist

Students should be able to explain:

* what source batches were generated
* why MinIO uses `s3a://` paths in Spark
* where bronze data is stored
* what incremental load means
* why small files are compacted
* how to validate row counts after loading

[Back to main README](../../README.md#data-lake-operations)
