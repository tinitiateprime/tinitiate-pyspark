# MinIO Source Files to PostgreSQL — Student Guide

This document explains the MinIO to PostgreSQL PySpark lab.

Before starting this lab, complete the local setup:

[`pyspark-basics/misc/pyspark-local-setup.md`](../pyspark-basics/misc/pyspark-local-setup.md)

The local setup covers Docker, PostgreSQL, MinIO, DBeaver, Python packages, and PostgreSQL table creation.

In this lab, students will upload the prepared files into MinIO and then load the MinIO files into PostgreSQL using PySpark.

The goal is to practice a real data engineering flow:

```text
CSV / JSON / Parquet files -> MinIO bucket -> PySpark -> PostgreSQL tables
```

Simple lab flow:

1. Upload the prepared data files into MinIO.
2. Check the files in the MinIO browser.
3. Run the PySpark load script to move data from MinIO into PostgreSQL.
4. Check the loaded rows in PostgreSQL or DBeaver.

This lab uses:

- PostgreSQL database `tinitiateai`
- PostgreSQL user `ti_dbuser`
- MinIO bucket `datalake`

Run all commands from the project folder:

```cmd
cd C:\tinitiate_pyspark
```

This folder is called the repository root. All commands in this guide should be run from this location.

## Source files for the lab

The recommended approach is to download the prepared data ZIP files from GitHub and extract them into:

```text
data/database_scenarios
```

The download and extraction steps are covered in the local setup guide:

[`../pyspark-basics/misc/pyspark-local-setup.md`](../pyspark-basics/misc/pyspark-local-setup.md)

If students want to generate the source files locally instead of using the GitHub files, use this optional guide:

[`GENERATE_FILES_LOCALLY.md`](GENERATE_FILES_LOCALLY.md)

## Step 1: Upload the GitHub files to MinIO

If students downloaded and extracted the prepared files from GitHub, the files should already exist under:

```text
data/database_scenarios
```

To upload those existing files to MinIO without generating them again, run:

```cmd
C:\Python311\python.exe pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

Or:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

What happens:

1. The script uses the existing files under `data/database_scenarios`.
2. DDL is staged at `data/database_scenarios/DDL/ddl.sql`.
3. The script connects to MinIO:

```text
Endpoint: http://localhost:9000
User: minio
Password: minio123
Bucket: datalake
```

4. The DDL and scenario folders are uploaded to MinIO.

If students want to generate the files locally instead of using the GitHub ZIP files, follow this optional guide:

[`GENERATE_FILES_LOCALLY.md`](GENERATE_FILES_LOCALLY.md)

## Step 2: Check files in MinIO

Open:

```text
http://localhost:9001
```

Login:

```text
Username: minio
Password: minio123
```

Open bucket:

```text
datalake
```

You should see:

```text
DDL
01_many_small_json_customer
02_many_small_json_multiple_tables
...
10_ultra_one_million_files
```

Inside each scenario, files are separated by format:

```text
<scenario-folder>
  <dataset-folder>
    csv
    json
    parquet
```

Example:

```text
datalake
  01_many_small_json_customer
    01_json_small_customer
      csv
        customer
      json
        customer
      parquet
        customer
```

## Step 3: Load data from MinIO to PostgreSQL using PySpark

In this step, PySpark reads files from MinIO and writes the rows into PostgreSQL.

Run these variables first in the same Command Prompt window:

```cmd
set POSTGRES_JDBC_URL=jdbc:postgresql://localhost:5432/tinitiateai
set POSTGRES_USER=ti_dbuser
set POSTGRES_PASSWORD=tiuser!23456

set MINIO_ENDPOINT=http://localhost:9000
set MINIO_ACCESS_KEY=minio
set MINIO_SECRET_KEY=minio123
set MINIO_BUCKET=datalake

set PACKAGES=org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4
```

These values tell PySpark:

- where PostgreSQL is running;
- which PostgreSQL user and password to use;
- where MinIO is running;
- which extra Spark packages are needed to read from MinIO and write to PostgreSQL.

### Python script used to load scenario folders

The full reusable scripts are:

- [`pyspark-database/scripts/load_files_to_postgres.py`](scripts/load_files_to_postgres.py), for loading one MinIO folder into one PostgreSQL table;
- [`pyspark-database/scripts/load_minio_scenarios_to_postgres.py`](scripts/load_minio_scenarios_to_postgres.py), for loading multiple scenario folders.

Use [`load_minio_scenarios_to_postgres.py`](scripts/load_minio_scenarios_to_postgres.py) when students want to load many scenario folders from MinIO into PostgreSQL.

Students do not need to pass parameters for the normal lab run. By default, the script loads:

- source format: `csv`;
- scenarios: `all`;
- write mode: `overwrite`;
- MinIO bucket: `datalake`;
- PostgreSQL database: `tinitiateai`.

Do not run [`load_files_to_postgres.py`](scripts/load_files_to_postgres.py) by itself without arguments. It is a single-folder loader, so it must be given:

- `--source-path`
- `--source-format`
- `--target-table`

The basic scenario-loading logic is:

```python
from pyspark.sql import SparkSession

SCENARIOS = {
    "01": {
        "folder": "01_many_small_json_customer",
        "dataset": "01_json_small_customer",
        "tables": ["customer"],
    },
    "02": {
        "folder": "02_many_small_json_multiple_tables",
        "dataset": "02_json_small_multi",
        "tables": ["location", "product", "customer", "sales"],
    },
    "05": {
        "folder": "05_many_small_csv_multiple_tables",
        "dataset": "05_csv_small_multi",
        "tables": ["dept", "projects", "emp", "emp_projects"],
    },
}

source_format = "csv"
bucket = "datalake"

postgres_url = "jdbc:postgresql://localhost:5432/tinitiateai"
postgres_user = "ti_dbuser"
postgres_password = "tiuser!23456"

spark = (
    SparkSession.builder
    .appName("load-minio-scenarios-to-postgres")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

for scenario_no, scenario in SCENARIOS.items():
    for table in scenario["tables"]:
        source_path = (
            f"s3a://{bucket}/"
            f"{scenario['folder']}/"
            f"{scenario['dataset']}/"
            f"{source_format}/"
            f"{table}"
        )

        if source_format == "csv":
            df = spark.read.option("header", True).option("inferSchema", True).csv(source_path)
        elif source_format == "json":
            df = spark.read.json(source_path)
        else:
            df = spark.read.parquet(source_path)

        (
            df.write
            .format("jdbc")
            .option("url", postgres_url)
            .option("dbtable", f"training.{table}")
            .option("user", postgres_user)
            .option("password", postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

        print(f"Loaded {source_path} into training.{table}")

spark.stop()
```

The actual repository script has more features than the basic example above:

- supports all scenarios 01-10;
- loads CSV from all scenarios by default;
- optionally accepts `--source-format json` or `--source-format parquet`;
- optionally accepts selected scenarios like `--scenarios 01,02,05`;
- uses table-specific column casting from [`load_files_to_postgres.py`](scripts/load_files_to_postgres.py);
- writes rejected records and audit records.

The commands to execute the full script are shown in Option A below.

### Option A: Load all MinIO scenario folders

Use this option when you want PySpark to load multiple scenario folders from MinIO into PostgreSQL.

For the normal lab run, students only run this command:

```cmd
spark-submit --packages %PACKAGES% ^
  pyspark-database/scripts/load_minio_scenarios_to_postgres.py
```

This loads all CSV scenario folders into PostgreSQL using the default settings.

If the instructor wants to load JSON instead of CSV:

```cmd
spark-submit --packages %PACKAGES% ^
  pyspark-database/scripts/load_minio_scenarios_to_postgres.py ^
  --source-format json
```

If the instructor wants to load Parquet instead of CSV:

```cmd
spark-submit --packages %PACKAGES% ^
  pyspark-database/scripts/load_minio_scenarios_to_postgres.py ^
  --source-format parquet
```

What this script does:

1. Looks at the scenario list in the script.
2. Builds each MinIO path automatically.
3. Reads the selected format: CSV, JSON, or Parquet.
4. Loads each folder into the matching PostgreSQL table.
5. Writes audit records into `training.load_audit`.

Important: load one format at a time. CSV, JSON, and Parquet contain the same logical data. Loading all three formats together can duplicate data.

The default write mode is `overwrite`, which makes the lab easy to rerun. Some scenarios load into the same target table, such as `customer` or `sales`, so overwrite avoids duplicate primary-key errors.

If the instructor wants to load only selected scenarios, pass the scenario numbers:

```cmd
spark-submit --packages %PACKAGES% ^
  pyspark-database/scripts/load_minio_scenarios_to_postgres.py ^
  --source-format csv ^
  --scenarios 01,02,05 ^
  --write-mode overwrite
```

### Option B: Beginner example: load only Scenario 01 customer CSV files

This example loads the CSV version of Scenario 01 from MinIO into PostgreSQL table `training.customer`.

Before starting this example, make sure students already ran:

```cmd
C:\Python311\python.exe pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

JSON and Parquet work the same way; only the format folder and `--source-format` value change.

### 1. Confirm Scenario 01 exists in MinIO

In the MinIO browser at `http://localhost:9001`, open:

```text
datalake
  01_many_small_json_customer
    01_json_small_customer
      csv
      json
      parquet
```

The Spark path for the CSV customer files is:

```text
s3a://datalake/01_many_small_json_customer/01_json_small_customer/csv/customer
```

### 2. Load the CSV files from MinIO into PostgreSQL

```cmd
spark-submit --packages %PACKAGES% ^
  pyspark-database/scripts/load_files_to_postgres.py ^
  --source-path s3a://datalake/01_many_small_json_customer/01_json_small_customer/csv/customer ^
  --source-format csv ^
  --target-table customer ^
  --scenario scenario-01-minio-csv-customer ^
  --write-mode overwrite ^
  --expected-files 20 ^
  --write-partitions 4
```

Important: the `--source-path` starts with `s3a://datalake/...`. That means Spark reads from MinIO, not from the local generated folder.

What this PySpark command does:

1. Reads customer CSV files from MinIO.
2. Validates and converts the columns.
3. Writes good rows into PostgreSQL table `training.customer`.
4. Writes a load audit record into `training.load_audit`.

### 3. Validate the PostgreSQL table

In DBeaver, open a SQL Editor for the `tinitiateai` connection and run:

```sql
SELECT COUNT(*) FROM training.customer;
```

Or run the same check from Command Prompt:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "SELECT COUNT(*) FROM training.customer;"
```

Expected count:

```text
200
```

### 4. Validate the audit record

In DBeaver SQL Editor, run:

```sql
SELECT scenario, target_table, source_format, source_path, accepted_rows, rejected_rows
FROM training.load_audit
ORDER BY load_id DESC
LIMIT 5;
```

Or run it from Command Prompt:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "SELECT scenario, target_table, source_format, source_path, accepted_rows, rejected_rows FROM training.load_audit ORDER BY load_id DESC LIMIT 5;"
```

The `source_path` should begin with:

```text
s3a://datalake/
```

## Loading another format

To load JSON instead of CSV, change:

```text
csv/customer
--source-format csv
```

to:

```text
json/customer
--source-format json
```

To load Parquet, change it to:

```text
parquet/customer
--source-format parquet
```

Load only one format at a time. CSV, JSON, and Parquet contain the same logical rows, so loading all three into the same table will duplicate data unless you overwrite or clean the table first.

## Scenario path reference

Use these MinIO paths when building `--source-path`.

| Scenario | Table(s) | MinIO path pattern |
|---|---|---|
| 01 | `customer` | `s3a://datalake/01_many_small_json_customer/01_json_small_customer/<format>/customer` |
| 02 | `location`, `product`, `customer`, `sales` | `s3a://datalake/02_many_small_json_multiple_tables/02_json_small_multi/<format>/<table>` |
| 03 | `sales` | `s3a://datalake/03_many_large_json_sales/03_json_large_sales/<format>/sales` |
| 04 | `emp` | `s3a://datalake/04_many_small_csv_emp/04_csv_small_emp/<format>/emp` |
| 05 | `dept`, `projects`, `emp`, `emp_projects` | `s3a://datalake/05_many_small_csv_multiple_tables/05_csv_small_multi/<format>/<table>` |
| 06 | `emp` | `s3a://datalake/06_many_large_csv_emp/06_csv_large_emp/<format>/emp` |
| 07 | `sales_transaction` | `s3a://datalake/07_many_small_parquet_transaction/07_parquet_small_sales_transaction/<format>/sales_transaction` |
| 08 | `location`, `product`, `customer`, `sales` | `s3a://datalake/08_many_small_parquet_multiple_tables/08_parquet_small_multi/<format>/<table>` |
| 09 | `sales` | `s3a://datalake/09_many_large_parquet_sales/09_parquet_large_sales/<format>/sales` |
| 10 | `sales_transaction` | `s3a://datalake/10_ultra_one_million_files/10_ultra_sales_transaction/<format>/sales_transaction` |
| 11 | `sales_transaction`, CDC changes | `s3a://datalake/11_millions_updates_deletes/...` |

Use one of these values for `<format>`:

```text
csv
json
parquet
```

## Useful checks

Show PostgreSQL tables:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "\dt training.*"
```

Show recent loads:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "SELECT target_table, scenario, source_format, source_path, accepted_rows, rejected_rows FROM training.load_audit ORDER BY load_id DESC LIMIT 10;"
```

Show row count for one table:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "SELECT COUNT(*) FROM training.customer;"
```

## If you need to republish

Re-running [`publish_minio_lab.py`](scripts/publish_minio_lab.py) uploads the files again and replaces objects with the same names.

```cmd
C:\Python311\python.exe pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

If old folders were created with different names, delete them from the MinIO console before republishing.
