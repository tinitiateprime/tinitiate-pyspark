# Optional: Generate Source Files Locally and Upload to MinIO

Use this page only if students want to generate the source files on their own machine instead of using the files downloaded from GitHub.

The main lab guide assumes students download the prepared files from GitHub. This optional page explains what [`scripts/publish_minio_lab.py`](scripts/publish_minio_lab.py) does when students choose to generate the files locally.

## What `publish_minio_lab.py` does

When students run [`publish_minio_lab.py`](scripts/publish_minio_lab.py), it:

- generates source files for scenarios 01-10;
- creates CSV, JSON, and Parquet versions of the datasets;
- copies the PostgreSQL DDL to `data/database_scenarios/DDL/ddl.sql`;
- connects to MinIO at `http://localhost:9000`;
- creates the `datalake` bucket if it does not exist;
- uploads the DDL and scenario folders into MinIO.

After it finishes, MinIO should look like this:

```text
datalake
  DDL
    ddl.sql
  01_many_small_json_customer
  02_many_small_json_multiple_tables
  03_many_large_json_sales
  04_many_small_csv_emp
  05_many_small_csv_multiple_tables
  06_many_large_csv_emp
  07_many_small_parquet_transaction
  08_many_small_parquet_multiple_tables
  09_many_large_parquet_sales
  10_ultra_one_million_files
```

Scenario 11 is optional and heavier. It is not generated unless students use `--include-heavy`.

## Install Python packages

The local generator needs:

- `minio`, to upload files to MinIO;
- `pyarrow`, to create Parquet files.

Run:

```cmd
python -m pip install --user minio pyarrow
```

If `python` already points to Python 3, this also works:

```cmd
python -m pip install --user minio pyarrow
```

## Generate scenarios 01-10 and upload them to MinIO

Run this from the project root:

```cmd
python pyspark-database/scripts/publish_minio_lab.py
```

Or:

```cmd
python pyspark-database/scripts/publish_minio_lab.py
```

## Include Scenario 11

Scenario 11 is heavier because it creates larger update/delete datasets.

To include it, run:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --include-heavy
```

## Upload existing files without regenerating

If the files already exist under:

```text
data/database_scenarios
```

students can upload the existing files to MinIO without generating them again:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

Use this when the files were already downloaded from GitHub and only need to be uploaded into the local MinIO bucket.
