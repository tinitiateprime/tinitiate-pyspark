# Iceberg Bronze and Silver Layers in MinIO

[Home](../README.md) | [Data Lake](../DATA_LAKE.md) | [Setup](../SETUP.md)

This lab explains how to build a simple Data Lake table layer using PySpark, Apache Iceberg, and MinIO.

In the earlier lab, students loaded files into MinIO and then loaded data into PostgreSQL. In this lab, we add one important layer in between:

```text
Source files in MinIO
        ↓
Iceberg Bronze tables in MinIO
        ↓
Iceberg Silver tables in MinIO
        ↓
PostgreSQL or reporting layer
```

This is a common real-world pattern. Raw files first land in object storage, then a table format such as Iceberg makes those files easier to query, clean, track, and manage.

## Why we need Iceberg tables

MinIO stores files. For example, it can store:

- CSV files
- JSON files
- Parquet files

That is useful, but plain files do not behave like database tables.

For example, a plain folder of files does not automatically give us:

- table history
- schema tracking
- snapshots
- table-level metadata
- easy table reads using SQL style names
- better management for large data lake tables

Apache Iceberg solves this problem. Iceberg is a table format for data lakes.

Iceberg stores data as files, but it also stores metadata about those files. That metadata is what makes the folder work like a table.

## Simple explanation for students

Think of MinIO as the storage room.

Think of Iceberg as the catalog system inside the storage room.

Without Iceberg, we only have boxes of files.

With Iceberg, we know:

- which files belong to each table
- what columns the table has
- when the table changed
- where the latest version of the table is
- how Spark should read the table

## What students will build

Students will use the same files already generated for the PostgreSQL lab.

The files are stored in MinIO bucket:

```text
datalake
```

After this lab runs, the bucket will contain:

```text
datalake
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

  iceberg_warehouse
    bronze_layer
      customer
      dept
      emp
      emp_projects
      location
      product
      projects
      sales
      sales_transaction

    silver_layer
      customer
      dept
      emp
      emp_projects
      location
      product
      projects
      sales
      sales_transaction
```

The first group of folders are the source files.

The `iceberg_warehouse` folder contains Iceberg tables.

## Bronze and Silver layers

### Bronze layer

Bronze is the raw table layer.

In this lab, Bronze means:

- read files from MinIO
- keep the data close to the original source
- add load tracking columns
- store it as Iceberg tables

Example Bronze table:

```text
ti_iceberg.bronze_layer.customer
```

Bronze tables include these extra columns:

- `source_scenario`
- `source_table`
- `source_format`
- `bronze_load_ts`

These columns help students know where the data came from.

### Silver layer

Silver is the cleaned table layer.

In this lab, Silver means:

- read data from Bronze Iceberg tables
- trim spaces from text columns
- lowercase email values
- remove duplicate rows
- add a Silver load timestamp
- store the result as Iceberg tables

Example Silver table:

```text
ti_iceberg.silver_layer.customer
```

Silver tables include this extra column:

- `silver_load_ts`

## Iceberg names used in this lab

This lab uses:

| Item | Value | Meaning |
|---|---|---|
| MinIO bucket | `datalake` | Storage bucket where files and Iceberg tables are stored |
| Iceberg catalog | `ti_iceberg` | Spark name used to access Iceberg tables |
| Iceberg warehouse | `s3a://datalake/iceberg_warehouse` | MinIO location where Iceberg tables are stored |
| Bronze namespace | `bronze_layer` | Raw Iceberg table layer |
| Silver namespace | `silver_layer` | Cleaned Iceberg table layer |

In Spark, a full Iceberg table name looks like this:

```text
catalog.namespace.table
```

Example:

```text
ti_iceberg.bronze_layer.customer
```

This means:

```text
catalog:   ti_iceberg
namespace: bronze_layer
table:     customer
```

## Step 1: Start Docker

From the project folder, run:

```cmd
cd C:\Code\tinitiate-pyspark
docker compose -f pyspark-database/ti-data-engineering-docker-compose.yml up -d
```

Check that the containers are running:

```cmd
docker ps
```

Students should see containers like:

- `ti-batch-minio`
- `ti-batch-jupyter`
- `ti-batch-spark-master`
- `ti-batch-spark-worker`

## Step 2: Make sure source files are available in MinIO

Open MinIO:

```text
http://localhost:9001
```

Login:

```text
Username: minio
Password: minio123
```

Open the `datalake` bucket.

Students should see scenario folders like:

```text
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

If the folders are already there, continue to Step 3.

If the folders are missing, run:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

This uploads the already generated local scenario files into MinIO.

## Step 3: Run the Iceberg Bronze/Silver PySpark script

Run this command from the project folder:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py
```

The script file is:

[`pyspark-datalake/scripts/iceberg_bronze_silver.py`](scripts/iceberg_bronze_silver.py)

The first run may take a few minutes because Spark downloads the Iceberg and MinIO/S3 packages.

## Why the command uses packages

The command includes:

```text
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
```

This gives Spark Iceberg table support.

The command also includes:

```text
org.apache.hadoop:hadoop-aws:3.3.4
```

This gives Spark the `s3a://` connector needed to read and write MinIO data.

Without these packages, Spark can start, but it cannot work with Iceberg tables in MinIO.

## Step 4: What the PySpark code creates

The script creates the Iceberg catalog:

```python
.config("spark.sql.catalog.ti_iceberg", "org.apache.iceberg.spark.SparkCatalog")
.config("spark.sql.catalog.ti_iceberg.type", "hadoop")
.config("spark.sql.catalog.ti_iceberg.warehouse", "s3a://datalake/iceberg_warehouse")
```

This tells Spark:

```text
Create Iceberg tables under MinIO bucket datalake, folder iceberg_warehouse.
```

Then the script creates two Iceberg namespaces:

```sql
CREATE NAMESPACE IF NOT EXISTS ti_iceberg.bronze_layer;
CREATE NAMESPACE IF NOT EXISTS ti_iceberg.silver_layer;
```

Students can explain namespace as a schema or database area.

## Step 5: How Bronze tables are created

The script reads source files from MinIO.

Example source path:

```text
s3a://datalake/01_many_small_json_customer/01_json_small_customer/csv/customer
```

Then it writes the data as an Iceberg table:

```text
ti_iceberg.bronze_layer.customer
```

Conceptually, the code does this:

```python
bronze_frame = (
    source_frame
    .withColumn("source_scenario", lit("01"))
    .withColumn("source_table", lit("customer"))
    .withColumn("source_format", lit("csv"))
    .withColumn("bronze_load_ts", current_timestamp())
)

bronze_frame.writeTo("ti_iceberg.bronze_layer.customer").using("iceberg").create()
```

In MinIO, students will see folders under:

```text
datalake / iceberg_warehouse / bronze_layer
```

## Step 6: How Silver tables are created

The script reads from Bronze:

```text
ti_iceberg.bronze_layer.customer
```

Then it cleans the data:

- trims spaces from string columns
- lowercases email values
- removes duplicate rows
- adds `silver_load_ts`

Then it writes to Silver:

```text
ti_iceberg.silver_layer.customer
```

Conceptually, the code does this:

```python
silver_frame = (
    spark.table("ti_iceberg.bronze_layer.customer")
    .dropDuplicates()
    .withColumn("silver_load_ts", current_timestamp())
)

silver_frame.writeTo("ti_iceberg.silver_layer.customer").using("iceberg").create()
```

In MinIO, students will see folders under:

```text
datalake / iceberg_warehouse / silver_layer
```

## Step 7: Check the Iceberg files in MinIO

Open MinIO:

```text
http://localhost:9001
```

Go to:

```text
datalake / iceberg_warehouse
```

Students should see:

```text
bronze_layer
silver_layer
```

Inside each table folder, Iceberg stores files similar to:

```text
data
metadata
```

The `data` folder contains the actual data files.

The `metadata` folder contains Iceberg metadata files. These metadata files are what make the folder work like a table.

## Step 8: Run only one small example

For a beginner classroom demo, run only Scenario 01:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py --scenarios 01
```

This creates:

```text
ti_iceberg.bronze_layer.customer
ti_iceberg.silver_layer.customer
```

This is the best first demo because students can focus on one table.

## Step 9: Load JSON or Parquet instead of CSV

By default, the script reads CSV source files.

To read JSON files:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py --source-format json
```

To read Parquet files:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py --source-format parquet
```

## Step 10: How this connects to PostgreSQL

PostgreSQL is still useful for relational reporting, dashboards, and SQL practice.

But now students can understand the full flow:

```text
Raw files in MinIO
        ↓
Bronze Iceberg tables
        ↓
Silver Iceberg tables
        ↓
PostgreSQL tables
```

In a real project:

- Bronze keeps the raw landing data.
- Silver keeps cleaned and standardized data.
- PostgreSQL can store final serving tables for applications or reports.

## Classroom explanation flow

Use this order when teaching:

1. Show the source folders in MinIO.
2. Explain that they are just files.
3. Explain that Iceberg turns file folders into managed tables.
4. Run the Iceberg script.
5. Show the new `iceberg_warehouse` folder in MinIO.
6. Open `bronze_layer` and explain raw tables.
7. Open `silver_layer` and explain cleaned tables.
8. Explain that PostgreSQL can be loaded later from cleaned data.

## Common errors

If Spark says it cannot find `s3a`, make sure the command includes:

```text
org.apache.hadoop:hadoop-aws:3.3.4
```

If Spark says it cannot find Iceberg classes, make sure the command includes:

```text
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
```

If Spark says the bucket does not exist, run:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

If Spark says the script file does not exist, make sure the project folder is mounted into Docker and this file exists:

```text
/home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py
```

If Docker is not running, start Docker Desktop first and then run:

```cmd
docker compose -f pyspark-database/ti-data-engineering-docker-compose.yml up -d
```
