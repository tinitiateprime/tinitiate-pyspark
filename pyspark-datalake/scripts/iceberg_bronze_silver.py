#!/usr/bin/env python3
"""Create Iceberg Bronze and Silver tables in MinIO with PySpark.

Default student run:

    docker exec ti-batch-jupyter spark-submit --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py

This script uses the same MinIO source files created for the PostgreSQL lab.
It reads the scenario files from the datalake bucket, writes raw Iceberg tables
to the bronze_layer namespace, then writes cleaned Iceberg tables to the
silver_layer namespace.
"""

from __future__ import annotations

import argparse
import os
from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, lit, lower, trim
from pyspark.sql.types import StringType


SPARK_PACKAGES = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4"
CATALOG_NAME = "ti_iceberg"

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
    "03": {
        "folder": "03_many_large_json_sales",
        "dataset": "03_json_large_sales",
        "tables": ["sales"],
    },
    "04": {
        "folder": "04_many_small_csv_emp",
        "dataset": "04_csv_small_emp",
        "tables": ["emp"],
    },
    "05": {
        "folder": "05_many_small_csv_multiple_tables",
        "dataset": "05_csv_small_multi",
        "tables": ["dept", "projects", "emp", "emp_projects"],
    },
    "06": {
        "folder": "06_many_large_csv_emp",
        "dataset": "06_csv_large_emp",
        "tables": ["emp"],
    },
    "07": {
        "folder": "07_many_small_parquet_transaction",
        "dataset": "07_parquet_small_sales_transaction",
        "tables": ["sales_transaction"],
    },
    "08": {
        "folder": "08_many_small_parquet_multiple_tables",
        "dataset": "08_parquet_small_multi",
        "tables": ["location", "product", "customer", "sales"],
    },
    "09": {
        "folder": "09_many_large_parquet_sales",
        "dataset": "09_parquet_large_sales",
        "tables": ["sales"],
    },
    "10": {
        "folder": "10_ultra_one_million_files",
        "dataset": "10_ultra_sales_transaction",
        "tables": ["sales_transaction"],
    },
}


def running_inside_docker() -> bool:
    return os.path.exists("/.dockerenv")


def default_minio_endpoint() -> str:
    if "MINIO_ENDPOINT" in os.environ:
        return os.environ["MINIO_ENDPOINT"]
    if running_inside_docker():
        return "http://minio:9000"
    return "http://localhost:9000"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source-format", choices=["csv", "json", "parquet"], default="csv")
    parser.add_argument("--scenarios", default="all")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "datalake"))
    parser.add_argument("--warehouse-prefix", default="iceberg_warehouse")
    parser.add_argument("--minio-endpoint", default=default_minio_endpoint())
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    return parser.parse_args()


def selected_scenarios(value: str) -> list[str]:
    if value.lower() == "all":
        return list(SCENARIOS)

    selected = [item.strip().zfill(2) for item in value.split(",") if item.strip()]
    unknown = [item for item in selected if item not in SCENARIOS]
    if unknown:
        raise ValueError(f"Unknown scenario number(s): {', '.join(unknown)}")
    return selected


def stop_if_windows_python_without_hadoop() -> None:
    if os.name != "nt":
        return
    if os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir"):
        return

    raise SystemExit(
        "This script starts PySpark and writes Iceberg tables. On Windows, local PySpark needs HADOOP_HOME/winutils.exe.\n\n"
        "For this lab, run the script inside Docker with spark-submit instead:\n\n"
        "  docker exec ti-batch-jupyter spark-submit --packages \"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.4\" /home/jovyan/work/pyspark-datalake/scripts/iceberg_bronze_silver.py\n\n"
        "Run that command from the project folder after Docker is started."
    )


def build_spark(args: argparse.Namespace) -> SparkSession:
    warehouse_path = f"s3a://{args.bucket}/{args.warehouse_prefix}"

    return (
        SparkSession.builder
        .appName(f"minio-iceberg-bronze-silver-{args.source_format}")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", warehouse_path)
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(args.minio_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def build_source_path(args: argparse.Namespace, scenario: dict[str, object], table: str) -> str:
    return (
        f"s3a://{args.bucket}/"
        f"{scenario['folder']}/"
        f"{scenario['dataset']}/"
        f"{args.source_format}/"
        f"{table}"
    )


def read_source(spark: SparkSession, source_path: str, source_format: str) -> DataFrame:
    if source_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(source_path)
    if source_format == "json":
        return spark.read.json(source_path)
    return spark.read.parquet(source_path)


def add_bronze_columns(frame: DataFrame, scenario_number: str, table: str, source_format: str) -> DataFrame:
    return (
        frame
        .withColumn("source_scenario", lit(scenario_number))
        .withColumn("source_table", lit(table))
        .withColumn("source_format", lit(source_format))
        .withColumn("bronze_load_ts", current_timestamp())
    )


def clean_for_silver(frame: DataFrame) -> DataFrame:
    cleaned = frame

    for field in cleaned.schema.fields:
        if isinstance(field.dataType, StringType):
            column_name = field.name
            if column_name == "email":
                cleaned = cleaned.withColumn(column_name, lower(trim(col(column_name))))
            else:
                cleaned = cleaned.withColumn(column_name, trim(col(column_name)))

    return cleaned.dropDuplicates().withColumn("silver_load_ts", current_timestamp())


def write_iceberg_table(spark: SparkSession, table_name: str, frame: DataFrame) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    frame.writeTo(table_name).using("iceberg").create()


def union_frames(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right, allowMissingColumns=True), frames)


def main() -> None:
    args = parse_args()
    scenario_numbers = selected_scenarios(args.scenarios)
    stop_if_windows_python_without_hadoop()

    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Starting MinIO to Iceberg Bronze/Silver load")
        print(f"Source format: {args.source_format}")
        print(f"Scenarios: {', '.join(scenario_numbers)}")
        print(f"Iceberg warehouse: s3a://{args.bucket}/{args.warehouse_prefix}")

        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.bronze_layer")
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.silver_layer")

        bronze_frames_by_table: dict[str, list[DataFrame]] = {}

        for scenario_number in scenario_numbers:
            scenario = SCENARIOS[scenario_number]
            for table in scenario["tables"]:
                source_path = build_source_path(args, scenario, table)
                print(f"\nReading source files: {source_path}")
                source_frame = read_source(spark, source_path, args.source_format)
                bronze_frame = add_bronze_columns(source_frame, scenario_number, table, args.source_format)
                bronze_frames_by_table.setdefault(table, []).append(bronze_frame)

        for table, frames in bronze_frames_by_table.items():
            bronze_table = f"{CATALOG_NAME}.bronze_layer.{table}"
            silver_table = f"{CATALOG_NAME}.silver_layer.{table}"

            bronze_frame = union_frames(frames)
            print(f"\nWriting Bronze Iceberg table: {bronze_table}")
            write_iceberg_table(spark, bronze_table, bronze_frame)

            print(f"Writing Silver Iceberg table: {silver_table}")
            silver_frame = clean_for_silver(spark.table(bronze_table))
            write_iceberg_table(spark, silver_table, silver_frame)

        print("\nFinished creating Iceberg Bronze and Silver tables in MinIO.")
        print("Open MinIO and check: datalake / iceberg_warehouse / bronze_layer and silver_layer")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
