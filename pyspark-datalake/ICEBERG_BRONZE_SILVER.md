# Iceberg Bronze and Silver Layers in MinIO

[Home](../README.md) | [Data Lake](../DATA_LAKE.md) | [Setup](../SETUP.md)

This lab shows how to build a simple data lake using PySpark, Apache Iceberg, and MinIO.

The important idea is:

- The original source files are already stored in MinIO.
- PySpark reads those files from the `datalake` bucket.
- PySpark creates an Iceberg database area called `bronze_layer`.
- PySpark cleans the Bronze data and creates another Iceberg database area called `silver_layer`.

In this lab, MinIO is used like object storage. Iceberg tables are stored inside the same MinIO bucket.

## What students will build

After this lab runs, MinIO will have this structure:

```text
datalake
  01_many_small_json_customer
  02_many_small_json_multiple_tables
  ...
  iceberg_warehouse
    bronze_layer
      customer
      emp
      sales
      ...
    silver_layer
      customer
      emp
      sales
      ...
```

The scenario folders are the source files. The `iceberg_warehouse` folder is where Iceberg stores the Bronze and Silver tables.

## Step 1: Start Docker

From the project folder, run:

```cmd
cd C:\Code\tinitiate-pyspark
docker compose -f pyspark-database/ti-data-engineering-docker-compose.yml up -d
```

Check that the main containers are running:

```cmd
docker ps
```

Students should see containers such as:

- `ti-batch-minio`
- `ti-batch-jupyter`
- `ti-batch-spark-master`
- `ti-batch-spark-worker`

## Step 2: Put the source files in MinIO

If the scenario files are already visible in MinIO, skip this step.

If the files are not in MinIO yet, run:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --skip-generate
```

Open MinIO in the browser:

```text
http://localhost:9001
```

Login:

```text
Username: minio
Password: minio123
```

Open the `datalake` bucket. Students should see folders like:

- `01_many_small_json_customer`
- `02_many_small_json_multiple_tables`
- `03_many_large_json_sales`
- `04_many_small_csv_emp`
- `05_many_small_csv_multiple_tables`
- `06_many_large_csv_emp`
- `07_many_small_parquet_transaction`
- `08_many_small_parquet_multiple_tables`
- `09_many_large_parquet_sales`
- `10_ultra_one_million_files`

## Step 3: Run the Iceberg Bronze/Silver PySpark script

Run this command from the project folder:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py
```

The script file is:

[`pyspark-datalake/scripts/iceberg_bronze_silver.py`](scripts/iceberg_bronze_silver.py)

The first run may take a little longer because Spark downloads the Iceberg and MinIO/S3 connector packages.

## What the script does

The script runs these steps:

1. Connects Spark to MinIO.
2. Reads source files from the `datalake` bucket.
3. Creates an Iceberg catalog named `ti_iceberg`.
4. Creates an Iceberg namespace called `bronze_layer`.
5. Creates an Iceberg namespace called `silver_layer`.
6. Loads the source files into Bronze Iceberg tables.
7. Cleans string columns and removes duplicate rows.
8. Writes the cleaned data into Silver Iceberg tables.

Bronze tables keep the raw data plus load tracking columns:

- `source_scenario`
- `source_table`
- `source_format`
- `bronze_load_ts`

Silver tables contain cleaned data plus:

- `silver_load_ts`

## Step 4: Check the files in MinIO

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

Inside those folders, Iceberg stores table data and metadata.

## Run only one scenario

For a smaller beginner test, run only Scenario 01:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py --scenarios 01
```

This reads Scenario 01 and creates:

```text
bronze_layer.customer
silver_layer.customer
```

## Use JSON or Parquet instead of CSV

By default, the script reads the CSV version of each scenario.

To read JSON files:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py --source-format json
```

To read Parquet files:

```cmd
docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py --source-format parquet
```

## Common errors

If Spark says it cannot find `s3a`, make sure the command includes:

```text
org.apache.hadoop:hadoop-aws:3.3.4
```

If Spark says it cannot find Iceberg classes, make sure the command includes:

```text
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2
```

If Spark says the bucket does not exist, run the MinIO publish script first:

```cmd
python pyspark-database/scripts/publish_minio_lab.py --skip-generate
```
