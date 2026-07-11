#!/usr/bin/env python3
"""Apply file-based sales-transaction updates and deletes through PostgreSQL staging."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window


CDC_COLUMNS = ["operation", "transaction_id", "amount", "transaction_ts", "change_ts"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-path", required=True)
    parser.add_argument("--source-format", choices=["csv", "json", "parquet"], required=True)
    parser.add_argument(
        "--jdbc-url",
        default=os.environ.get("POSTGRES_JDBC_URL", "jdbc:postgresql://localhost:5432/tinitiateai"),
    )
    parser.add_argument("--db-user", default=os.environ.get("POSTGRES_USER", "ti_dbuser"))
    parser.add_argument("--db-password", default=os.environ.get("POSTGRES_PASSWORD", "tiuser!23456"))
    parser.add_argument("--write-partitions", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=10_000)
    parser.add_argument("--reject-path", default="data/database_rejects/cdc")
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    return parser.parse_args()


def read_changes(spark: SparkSession, args: argparse.Namespace):
    if args.source_format == "parquet":
        return spark.read.parquet(args.source_path).withColumn(
            "_corrupt_record", F.lit(None).cast("string")
        )

    schema = StructType(
        [StructField(column, StringType(), True) for column in CDC_COLUMNS]
        + [StructField("_corrupt_record", StringType(), True)]
    )
    reader = (
        spark.read
        .schema(schema)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
    )
    if args.source_format == "csv":
        return reader.option("header", True).csv(args.source_path)
    return reader.option("multiLine", False).json(args.source_path)


def validate_changes(frame):
    required = set(CDC_COLUMNS)
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"CDC source is missing columns: {', '.join(missing)}")

    normalized = (
        frame
        .withColumn("_source_file", F.input_file_name())
        .withColumn("_raw_payload", F.to_json(F.struct(*[F.col(column) for column in CDC_COLUMNS])))
        .withColumn("operation", F.upper(F.trim("operation")))
        .withColumn("transaction_id", F.col("transaction_id").cast("bigint"))
        .withColumn("amount", F.col("amount").cast("decimal(16,2)"))
        .withColumn("transaction_ts", F.col("transaction_ts").cast("timestamp"))
        .withColumn("change_ts", F.col("change_ts").cast("timestamp"))
    )

    error = (
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_record")
        .when(F.col("operation").isNull() | ~F.col("operation").isin("U", "D"), "invalid_operation")
        .when(F.col("transaction_id").isNull(), "invalid_transaction_id")
        .when(F.col("change_ts").isNull(), "invalid_change_ts")
        .when(
            (F.col("operation") == "U")
            & (F.col("amount").isNull() | F.col("transaction_ts").isNull()),
            "update_missing_values",
        )
    )
    return normalized.withColumn("_error_reason", error.otherwise(F.lit(None).cast("string")))


def jdbc_options(args: argparse.Namespace) -> dict[str, str]:
    return {
        "url": args.jdbc_url,
        "dbtable": "training.sales_transaction_changes_staging",
        "user": args.db_user,
        "password": args.db_password,
        "driver": "org.postgresql.Driver",
        "batchsize": str(args.batch_size),
        "isolationLevel": "READ_COMMITTED",
    }


def stage_changes(good, args: argparse.Namespace) -> None:
    (
        good
        .select(*CDC_COLUMNS)
        .repartition(args.write_partitions)
        .write
        .format("jdbc")
        .options(**jdbc_options(args))
        .option("numPartitions", str(args.write_partitions))
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def apply_staged_changes(spark: SparkSession, args: argparse.Namespace) -> tuple[int, int]:
    result = (
        spark.read
        .format("jdbc")
        .option("url", args.jdbc_url)
        .option("query", "SELECT * FROM training.apply_sales_transaction_changes()")
        .option("user", args.db_user)
        .option("password", args.db_password)
        .option("driver", "org.postgresql.Driver")
        .load()
        .first()
    )
    if result is None:
        raise RuntimeError("PostgreSQL CDC function returned no result.")
    return int(result["updated_rows"]), int(result["deleted_rows"])


def main() -> None:
    args = parse_args()
    if args.write_partitions <= 0 or args.batch_size <= 0:
        raise ValueError("--write-partitions and --batch-size must be greater than zero.")

    builder = SparkSession.builder.appName("apply-sales-transaction-cdc")
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
        validated = validate_changes(read_changes(spark, args)).cache()
        rejected = validated.where(F.col("_error_reason").isNotNull())
        good = validated.where(F.col("_error_reason").isNull())
        rejected_rows = rejected.count()
        good_rows = good.count()

        if rejected_rows:
            (
                rejected
                .select("_source_file", "_error_reason", "_raw_payload", "_corrupt_record")
                .write
                .mode("append")
                .parquet(args.reject_path)
            )

        latest_change = (
            good
            .withColumn(
                "_change_rank",
                F.row_number().over(
                    Window.partitionBy("transaction_id").orderBy(F.col("change_ts").desc())
                ),
            )
            .where(F.col("_change_rank") == 1)
            .drop("_change_rank")
        )
        staged_rows = latest_change.count()
        stage_changes(latest_change, args)
        updated_rows, deleted_rows = apply_staged_changes(spark, args)
        print(
            {
                "accepted_changes": good_rows,
                "staged_latest_changes": staged_rows,
                "rejected_changes": rejected_rows,
                "updated_target_rows": updated_rows,
                "deleted_target_rows": deleted_rows,
                "unmatched_target_changes": staged_rows - updated_rows - deleted_rows,
            }
        )
    finally:
        if "validated" in locals():
            validated.unpersist()
        spark.stop()


if __name__ == "__main__":
    main()
