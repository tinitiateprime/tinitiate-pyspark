# 02 MinIO Setup

[Back to main README](../../README.md#data-lake-operations)

This folder explains how MinIO is used as the object store for the PySpark data lake lessons.

MinIO is S3-compatible storage. Spark accesses MinIO with `s3a://` paths and MinIO endpoint settings.

## Local MinIO Values

The examples use these defaults:

| Setting | Value |
|---|---|
| Endpoint | `http://localhost:9000` |
| Console | `http://localhost:9001` |
| Access key | `minioadmin` |
| Secret key | `minioadmin` |
| Bucket | `datalake` |

## Environment Variables

PowerShell:

```powershell
$env:MINIO_ENDPOINT = "http://localhost:9000"
$env:MINIO_ACCESS_KEY = "minioadmin"
$env:MINIO_SECRET_KEY = "minioadmin"
$env:MINIO_BUCKET = "datalake"
```

These variables are read by `pyspark-datalake/scripts/batch_load_to_minio.py`.

## Create the Bucket

Using the MinIO console:

1. Open `http://localhost:9001`.
2. Login with `minioadmin` / `minioadmin`.
3. Create a bucket named `datalake`.

Using the MinIO client:

```powershell
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/datalake
```

## Spark MinIO Configuration

The batch loader configures Spark like this:

```python
.config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
.config("spark.hadoop.fs.s3a.access.key", minio_access_key)
.config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

Important point:

* The path starts with `s3a://`, but the storage service is MinIO because the endpoint is MinIO.

## Target Data Lake Layout

Bronze zone:

```text
s3a://datalake/retail_banking/bronze/<table>/batch_date=YYYY-MM-DD/
```

Compacted bronze zone:

```text
s3a://datalake/retail_banking/bronze_compacted/<table>/batch_date=YYYY-MM-DD/
```

Future silver zone:

```text
s3a://datalake/retail_banking/silver/<business_entity>/
```

## Spark Package Note

If your Spark installation does not already include the S3A connector, use:

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  ...
```

Use the `hadoop-aws` version that matches your Spark and Hadoop runtime.

[Back to main README](../../README.md#data-lake-operations)
