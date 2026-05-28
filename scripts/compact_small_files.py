#!/usr/bin/env python3
"""Compact many small Spark input files into fewer larger files."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read small CSV, JSON, or Parquet files and write fewer compacted output files."
    )
    parser.add_argument("--format", choices=["csv", "json", "parquet"], required=True)
    parser.add_argument("--input-path", required=True, help="Input file or folder path.")
    parser.add_argument("--output-path", required=True, help="Output folder for compacted files.")
    parser.add_argument("--partitions", type=int, default=100, help="Number of output partitions/files to target.")
    parser.add_argument("--app-name", default="compact-small-files")
    return parser.parse_args()


def read_data(spark: SparkSession, fmt: str, input_path: str):
    if fmt == "csv":
        return spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    if fmt == "json":
        return spark.read.json(input_path)
    if fmt == "parquet":
        return spark.read.parquet(input_path)
    raise ValueError(f"Unsupported format: {fmt}")


def write_data(df, fmt: str, output_path: str) -> None:
    writer = df.write.mode("overwrite")
    if fmt == "csv":
        writer.option("header", True).csv(output_path)
        return
    if fmt == "json":
        writer.json(output_path)
        return
    if fmt == "parquet":
        writer.parquet(output_path)
        return
    raise ValueError(f"Unsupported format: {fmt}")


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = read_data(spark, args.format, args.input_path)
    row_count = df.count()
    compacted = df.repartition(args.partitions)
    write_data(compacted, args.format, args.output_path)

    print(f"Read {row_count:,} rows from {args.input_path}")
    print(f"Wrote compacted {args.format} data to {args.output_path}")
    print(f"Target output partitions: {args.partitions}")

    spark.stop()


if __name__ == "__main__":
    main()
