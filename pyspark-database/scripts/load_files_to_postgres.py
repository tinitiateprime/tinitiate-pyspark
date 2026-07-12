#!/usr/bin/env python3
"""Load CSV, JSON, or Parquet source files into PostgreSQL with PySpark JDBC."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from time import perf_counter

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType


TABLE_TYPES = {
    "location": {"location_id": "bigint", "location_name": "string", "region": "string"},
    "product": {
        "product_id": "bigint",
        "product_name": "string",
        "category": "string",
        "unit_price": "decimal(14,2)",
        "active": "boolean",
    },
    "customer": {
        "customer_id": "bigint",
        "customer_name": "string",
        "email": "string",
        "location_id": "bigint",
        "created_at": "timestamp",
    },
    "sales": {
        "sale_id": "bigint",
        "customer_id": "bigint",
        "product_id": "bigint",
        "location_id": "bigint",
        "quantity": "int",
        "unit_price": "decimal(14,2)",
        "sale_ts": "timestamp",
    },
    "dept": {"deptno": "bigint", "dname": "string", "loc": "string"},
    "emp": {
        "empno": "bigint",
        "ename": "string",
        "job": "string",
        "mgr": "bigint",
        "hiredate": "date",
        "sal": "decimal(14,2)",
        "commission": "decimal(14,2)",
        "deptno": "bigint",
    },
    "projects": {
        "project_id": "bigint",
        "project_name": "string",
        "budget": "decimal(16,2)",
        "location_id": "bigint",
    },
    "emp_projects": {
        "emp_project_id": "bigint",
        "empno": "bigint",
        "project_id": "bigint",
        "start_date": "date",
        "end_date": "date",
    },
    "sales_transaction": {
        "transaction_id": "bigint",
        "customer_id": "bigint",
        "product_id": "bigint",
        "location_id": "bigint",
        "amount": "decimal(16,2)",
        "transaction_ts": "timestamp",
    },
}


PRIMARY_KEYS = {
    "location": "location_id",
    "product": "product_id",
    "customer": "customer_id",
    "sales": "sale_id",
    "dept": "deptno",
    "emp": "empno",
    "projects": "project_id",
    "emp_projects": "emp_project_id",
    "sales_transaction": "transaction_id",
}


NULLABLE_COLUMNS = {"customer": {"email", "location_id"}, "emp": {"mgr", "commission"}, "emp_projects": {"end_date"}}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  Load one CSV MinIO folder into PostgreSQL table training.customer:

    spark-submit --packages %PACKAGES% pyspark-database/scripts/load_files_to_postgres.py ^
      --source-path s3a://datalake/01_many_small_json_customer/01_json_small_customer/csv/customer ^
      --source-format csv ^
      --target-table customer ^
      --scenario scenario-01-customer-csv ^
      --write-mode overwrite

  For the normal student lab, load multiple scenario folders without parameters:

    C:\\Python311\\python.exe pyspark-database/scripts/load_minio_scenarios_to_postgres.py
""",
    )
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--source-format", choices=["csv", "json", "parquet"], required=True)
    parser.add_argument("--target-table", choices=sorted(TABLE_TYPES), required=True)
    parser.add_argument("--scenario", default="manual")
    parser.add_argument(
        "--jdbc-url",
        default=os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/tinitiateai"),
    )
    parser.add_argument("--db-user", default=os.environ.get("POSTGRES_USER", "ti_dbuser"))
    parser.add_argument("--db-password", default=os.environ.get("POSTGRES_PASSWORD", "tiuser!23456"))
    parser.add_argument("--write-mode", choices=["append", "overwrite"], default="append")
    parser.add_argument("--write-partitions", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--expected-files", type=int)
    parser.add_argument("--reject-path", default="data/database_rejects")
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.write_partitions <= 0:
        raise ValueError("--write-partitions must be greater than zero.")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than zero.")


def raw_schema(table: str) -> StructType:
    fields = [StructField(column, StringType(), True) for column in TABLE_TYPES[table]]
    fields.append(StructField("_corrupt_record", StringType(), True))
    return StructType(fields)


def read_source(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.source_format == "parquet":
        frame = spark.read.parquet(args.source_path)
        return frame.withColumn("_corrupt_record", F.lit(None).cast("string"))

    reader = (
        spark.read
        .schema(raw_schema(args.target_table))
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )
    if args.source_format == "csv":
        return reader.option("header", True).csv(args.source_path)
    return reader.option("multiLine", False).json(args.source_path)


def normalize_and_validate(frame: DataFrame, table: str) -> DataFrame:
    expected = TABLE_TYPES[table]
    missing = sorted(set(expected) - set(frame.columns))
    if missing:
        raise ValueError(f"Source for {table} is missing columns: {', '.join(missing)}")

    with_metadata = (
        frame
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_raw_payload", F.to_json(F.struct(*[F.col(column) for column in expected])))
    )
    normalized = with_metadata
    for column, data_type in expected.items():
        normalized = normalized.withColumn(column, F.col(column).cast(data_type))

    error = F.when(F.col("_corrupt_record").isNotNull(), F.lit("malformed_record"))
    required = set(expected) - NULLABLE_COLUMNS.get(table, set())
    for column in sorted(required):
        error = error.when(F.col(column).isNull(), F.lit(f"invalid_or_missing_{column}"))

    return normalized.withColumn("_error_reason", error.otherwise(F.lit(None).cast("string")))


def jdbc_options(args: argparse.Namespace, table: str) -> dict[str, str]:
    return {
        "url": args.jdbc_url,
        "dbtable": f"training.{table}",
        "user": args.db_user,
        "password": args.db_password,
        "driver": "org.postgresql.Driver",
        "batchsize": str(args.batch_size),
        "isolationLevel": "READ_COMMITTED",
    }


def write_target(frame: DataFrame, args: argparse.Namespace) -> None:
    columns = list(TABLE_TYPES[args.target_table])
    writer = (
        frame
        .select(*columns)
        .repartition(args.write_partitions)
        .write
        .format("jdbc")
        .options(**jdbc_options(args, args.target_table))
        .option("numPartitions", str(args.write_partitions))
    )
    if args.write_mode == "overwrite":
        writer = writer.option("truncate", "true")
    writer.mode(args.write_mode).save()


def write_audit(spark: SparkSession, args: argparse.Namespace, metrics: dict[str, object]) -> None:
    audit = spark.createDataFrame([metrics])
    (
        audit.write
        .format("jdbc")
        .options(**jdbc_options(args, "load_audit"))
        .mode("append")
        .save()
    )


def run_load(spark: SparkSession, args: argparse.Namespace) -> None:
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)

    try:
        read_started = perf_counter()
        validated = normalize_and_validate(read_source(spark, args), args.target_table)
        validated.persist(StorageLevel.MEMORY_AND_DISK)
        accepted = validated.where(F.col("_error_reason").isNull())
        rejected = validated.where(F.col("_error_reason").isNotNull())
        accepted_rows = accepted.count()
        rejected_rows = rejected.count()
        read_seconds = perf_counter() - read_started

        if rejected_rows:
            (
                rejected
                .select("_source_file", "_error_reason", "_raw_payload", "_corrupt_record")
                .write
                .mode("append")
                .parquet(f"{args.reject_path}/{args.scenario}/{args.target_table}")
            )

        write_started = perf_counter()
        write_target(accepted, args)
        write_seconds = perf_counter() - write_started
        completed_at = datetime.now(timezone.utc).replace(tzinfo=None)

        write_audit(
            spark,
            args,
            {
                "scenario": args.scenario,
                "target_table": args.target_table,
                "source_format": args.source_format,
                "source_path": args.source_path,
                "source_files": args.expected_files if args.expected_files is not None else -1,
                "accepted_rows": accepted_rows,
                "rejected_rows": rejected_rows,
                "read_seconds": round(read_seconds, 3),
                "write_seconds": round(write_seconds, 3),
                "started_at": started_at,
                "completed_at": completed_at,
            },
        )

        print(
            {
                "table": args.target_table,
                "accepted_rows": accepted_rows,
                "rejected_rows": rejected_rows,
                "read_seconds": round(read_seconds, 3),
                "write_seconds": round(write_seconds, 3),
                "write_partitions": args.write_partitions,
            }
        )
    finally:
        if "validated" in locals():
            validated.unpersist()


def main() -> None:
    args = parse_args()
    validate_args(args)
    builder = SparkSession.builder.appName(f"files-to-postgres-{args.target_table}")
    if args.source_path.startswith("s3a://"):
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(args.minio_endpoint.startswith("https")).lower())
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        run_load(spark, args)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
