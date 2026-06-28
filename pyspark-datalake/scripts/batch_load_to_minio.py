#!/usr/bin/env python3
"""Batch load retail banking files into MinIO with PySpark."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


TABLES = [
    "customers",
    "account_plans",
    "accounts",
    "loan_types",
    "loans",
    "salary_deposits",
    "loan_payments",
    "payment_reminders",
    "account_transactions",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load retail banking batches from local source folders to MinIO.")
    parser.add_argument("--source-root", default="pyspark-datalake/data/source/retail_banking")
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "datalake"))
    parser.add_argument("--domain", default="retail_banking")
    parser.add_argument("--zone", choices=["bronze", "silver"], default="bronze")
    parser.add_argument("--source-format", choices=["csv", "json", "parquet"], default="csv")
    parser.add_argument("--target-format", choices=["parquet", "csv", "json"], default="parquet")
    parser.add_argument("--load-type", choices=["bulk", "incremental", "compact"], default="incremental")
    parser.add_argument("--batch-date", help="Required for incremental and compact loads. Example: 2026-01-01")
    parser.add_argument("--tables", nargs="*", default=TABLES)
    parser.add_argument("--output-partitions", type=int, default=8)
    parser.add_argument("--mode", choices=["append", "overwrite"], default="append")
    parser.add_argument("--minio-endpoint", default=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--minio-access-key", default=os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--minio-secret-key", default=os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    return parser.parse_args()


def build_spark(args: argparse.Namespace) -> SparkSession:
    return (
        SparkSession.builder.appName(f"{args.domain}-{args.zone}-{args.load_type}-load")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.fs.s3a.endpoint", args.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", args.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", args.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(args.minio_endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def source_path(args: argparse.Namespace, table: str) -> str:
    root = Path(args.source_root) / table
    if args.load_type == "bulk":
        return str(root)
    if not args.batch_date:
        raise ValueError("--batch-date is required for incremental and compact loads.")
    return str(root / f"batch_date={args.batch_date}")


def target_path(args: argparse.Namespace, table: str) -> str:
    if args.load_type == "compact":
        return f"s3a://{args.bucket}/{args.domain}/{args.zone}_compacted/{table}/batch_date={args.batch_date}"
    return f"s3a://{args.bucket}/{args.domain}/{args.zone}/{table}"


def read_source(spark: SparkSession, args: argparse.Namespace, table: str):
    path = source_path(args, table)
    if args.source_format == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(path)
    if args.source_format == "json":
        return spark.read.json(path)
    if args.source_format == "parquet":
        return spark.read.parquet(path)
    raise ValueError(args.source_format)


def write_target(df, args: argparse.Namespace, table: str) -> None:
    output = target_path(args, table)
    writer = df.repartition(args.output_partitions).write.mode(args.mode)
    if args.load_type in {"bulk", "incremental"}:
        writer = writer.partitionBy("batch_date")

    if args.target_format == "parquet":
        writer.parquet(output)
    elif args.target_format == "csv":
        writer.option("header", True).csv(output)
    elif args.target_format == "json":
        writer.json(output)
    else:
        raise ValueError(args.target_format)


def main() -> None:
    args = parse_args()
    spark = build_spark(args)
    spark.sparkContext.setLogLevel("WARN")

    for table in args.tables:
        df = read_source(spark, args, table)
        if "batch_date" not in df.columns:
            if not args.batch_date:
                raise ValueError(f"{table} has no batch_date column and --batch-date was not provided.")
            df = df.withColumn("batch_date", F.lit(args.batch_date))

        rows = df.count()
        write_target(df, args, table)
        print(f"{table}: loaded {rows:,} rows to {target_path(args, table)}")

    spark.stop()


if __name__ == "__main__":
    main()
