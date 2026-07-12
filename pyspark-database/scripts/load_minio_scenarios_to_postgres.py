#!/usr/bin/env python3
"""Load multiple MinIO scenario folders into PostgreSQL using PySpark."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from types import SimpleNamespace

from pyspark.sql import SparkSession

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import load_files_to_postgres as loader  # noqa: E402


DEFAULT_SPARK_PACKAGES = "org.postgresql:postgresql:42.7.4,org.apache.hadoop:hadoop-aws:3.3.4"


SCENARIO_LOADS = {
    "01": {
        "name": "01_many_small_json_customer",
        "dataset": "01_json_small_customer",
        "tables": ["customer"],
    },
    "02": {
        "name": "02_many_small_json_multiple_tables",
        "dataset": "02_json_small_multi",
        "tables": ["location", "product", "customer", "sales"],
    },
    "03": {
        "name": "03_many_large_json_sales",
        "dataset": "03_json_large_sales",
        "tables": ["sales"],
    },
    "04": {
        "name": "04_many_small_csv_emp",
        "dataset": "04_csv_small_emp",
        "tables": ["emp"],
    },
    "05": {
        "name": "05_many_small_csv_multiple_tables",
        "dataset": "05_csv_small_multi",
        "tables": ["dept", "projects", "emp", "emp_projects"],
    },
    "06": {
        "name": "06_many_large_csv_emp",
        "dataset": "06_csv_large_emp",
        "tables": ["emp"],
    },
    "07": {
        "name": "07_many_small_parquet_transaction",
        "dataset": "07_parquet_small_sales_transaction",
        "tables": ["sales_transaction"],
    },
    "08": {
        "name": "08_many_small_parquet_multiple_tables",
        "dataset": "08_parquet_small_multi",
        "tables": ["location", "product", "customer", "sales"],
    },
    "09": {
        "name": "09_many_large_parquet_sales",
        "dataset": "09_parquet_large_sales",
        "tables": ["sales"],
    },
    "10": {
        "name": "10_ultra_one_million_files",
        "dataset": "10_ultra_sales_transaction",
        "tables": ["sales_transaction"],
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Default student command:

  C:\\Python311\\python.exe pyspark-database/scripts/load_minio_scenarios_to_postgres.py

The default command loads CSV files from all scenarios into PostgreSQL with overwrite mode.

Optional instructor examples:

  Load JSON instead:

    C:\\Python311\\python.exe pyspark-database/scripts/load_minio_scenarios_to_postgres.py --source-format json

  Load selected scenarios only:

    C:\\Python311\\python.exe pyspark-database/scripts/load_minio_scenarios_to_postgres.py --scenarios 01,02,05
""",
    )
    parser.add_argument(
        "--source-format",
        choices=["csv", "json", "parquet"],
        default="csv",
        help="Load one file format at a time. Default: csv.",
    )
    parser.add_argument(
        "--scenarios",
        default="all",
        help="Comma-separated scenario numbers, for example 01,02,05. Default: all.",
    )
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "datalake"))
    parser.add_argument("--write-mode", choices=["append", "overwrite"], default="overwrite")
    parser.add_argument("--write-partitions", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument(
        "--jdbc-url",
        default=os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/tinitiateai"),
    )
    parser.add_argument("--db-user", default=os.environ.get("POSTGRES_USER", "ti_dbuser"))
    parser.add_argument("--db-password", default=os.environ.get("POSTGRES_PASSWORD", "tiuser!23456"))
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    parser.add_argument("--reject-path", default="data/database_rejects")
    parser.add_argument("--spark-packages", default=os.environ.get("SPARK_PACKAGES", DEFAULT_SPARK_PACKAGES))
    return parser.parse_args()


def selected_scenarios(value: str) -> list[str]:
    if value.lower() == "all":
        return list(SCENARIO_LOADS)

    selected = [item.strip().zfill(2) for item in value.split(",") if item.strip()]
    unknown = [item for item in selected if item not in SCENARIO_LOADS]
    if unknown:
        raise ValueError(f"Unknown scenario number(s): {', '.join(unknown)}")
    return selected


def source_path(bucket: str, scenario: dict[str, object], source_format: str, table: str) -> str:
    return (
        f"s3a://{bucket}/"
        f"{scenario['name']}/"
        f"{scenario['dataset']}/"
        f"{source_format}/"
        f"{table}"
    )


def build_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"load-minio-scenarios-{args.source_format}")
        .config("spark.jars.packages", args.spark_packages)
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(args.minio_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def load_one(spark: SparkSession, args: argparse.Namespace, scenario_number: str, table: str) -> None:
    scenario = SCENARIO_LOADS[scenario_number]
    path = source_path(args.bucket, scenario, args.source_format, table)
    scenario_name = f"{scenario_number}-{scenario['name']}-{args.source_format}-{table}"
    load_args = SimpleNamespace(
        source_path=path,
        source_format=args.source_format,
        target_table=table,
        scenario=scenario_name,
        jdbc_url=args.jdbc_url,
        db_user=args.db_user,
        db_password=args.db_password,
        write_mode=args.write_mode,
        write_partitions=args.write_partitions,
        batch_size=args.batch_size,
        expected_files=None,
        reject_path=args.reject_path,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
    )

    print(f"\nLoading {path} -> training.{table} ({args.write_mode})")
    loader.validate_args(load_args)
    loader.run_load(spark, load_args)


def main() -> None:
    args = parse_args()
    scenario_numbers = selected_scenarios(args.scenarios)
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    try:
        for scenario_number in scenario_numbers:
            for table in SCENARIO_LOADS[scenario_number]["tables"]:
                load_one(spark, args, scenario_number, table)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
