#!/usr/bin/env python3
"""Load MinIO scenario files into PostgreSQL with PySpark.

Default student run:

    docker exec ti-batch-jupyter python /home/jovyan/work/pyspark-database/scripts/postgres.py

No parameters are required for the normal lab. By default this script loads:

    format: csv
    scenarios: all
    bucket: datalake
    PostgreSQL database: tinitiateai
    PostgreSQL schema: training
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession


SPARK_PACKAGES = "org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4"

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
        return "http://ti-batch-minio:9000"
    return "http://localhost:9000"


def default_postgres_jdbc_url() -> str:
    if "POSTGRES_JDBC_URL" in os.environ:
        return os.environ["POSTGRES_JDBC_URL"]
    if running_inside_docker():
        return "jdbc:postgresql://ti-batch-postgres:5432/tinitiateai"
    return "jdbc:postgresql://localhost:5432/tinitiateai"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--source-format", choices=["csv", "json", "parquet"], default="csv")
    parser.add_argument("--scenarios", default="all")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "datalake"))
    parser.add_argument("--minio-endpoint", default=default_minio_endpoint())
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    parser.add_argument(
        "--jdbc-url",
        default=default_postgres_jdbc_url(),
    )
    parser.add_argument("--db-user", default=os.environ.get("POSTGRES_USER", "ti_dbuser"))
    parser.add_argument("--db-password", default=os.environ.get("POSTGRES_PASSWORD", "tiuser!23456"))
    return parser.parse_args()


def selected_scenarios(value: str) -> list[str]:
    if value.lower() == "all":
        return list(SCENARIOS)

    selected = [item.strip().zfill(2) for item in value.split(",") if item.strip()]
    unknown = [item for item in selected if item not in SCENARIOS]
    if unknown:
        raise ValueError(f"Unknown scenario number(s): {', '.join(unknown)}")
    return selected


def build_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"minio-to-postgres-{args.source_format}")
        .config("spark.jars.packages", SPARK_PACKAGES)
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(args.minio_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def stop_if_windows_python_without_hadoop() -> None:
    if os.name != "nt":
        return
    if os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir"):
        return

    raise SystemExit(
        "This script starts PySpark. On Windows, local PySpark needs HADOOP_HOME/winutils.exe.\n\n"
        "For this lab, run the script inside Docker instead:\n\n"
        "  docker exec ti-batch-jupyter python /home/jovyan/work/pyspark-database/scripts/postgres.py\n\n"
        "Run that command from the project folder after Docker is started."
    )


def build_source_path(args: argparse.Namespace, scenario: dict[str, object], table: str) -> str:
    return (
        f"s3a://{args.bucket}/"
        f"{scenario['folder']}/"
        f"{scenario['dataset']}/"
        f"{args.source_format}/"
        f"{table}"
    )


def read_source(spark: SparkSession, source_path: str, source_format: str):
    if source_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(source_path)
    if source_format == "json":
        return spark.read.json(source_path)
    return spark.read.parquet(source_path)


def write_to_postgres(frame, args: argparse.Namespace, table: str) -> None:
    (
        frame.write
        .format("jdbc")
        .option("url", args.jdbc_url)
        .option("dbtable", f"training.{table}")
        .option("user", args.db_user)
        .option("password", args.db_password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main() -> None:
    args = parse_args()
    scenario_numbers = selected_scenarios(args.scenarios)
    stop_if_windows_python_without_hadoop()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("Starting MinIO to PostgreSQL load")
        print(f"Source format: {args.source_format}")
        print(f"Scenarios: {', '.join(scenario_numbers)}")
        print(f"PostgreSQL: {args.jdbc_url}")

        for scenario_number in scenario_numbers:
            scenario = SCENARIOS[scenario_number]
            for table in scenario["tables"]:
                source_path = build_source_path(args, scenario, table)
                print(f"\nLoading {source_path} -> training.{table}")
                frame = read_source(spark, source_path, args.source_format)
                write_to_postgres(frame, args, table)
                print(f"Loaded training.{table}")

        print("\nFinished loading MinIO data into PostgreSQL.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
