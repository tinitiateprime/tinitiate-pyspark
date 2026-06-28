#!/usr/bin/env python3
"""Generate scalable file-I/O fixtures for PySpark performance labs."""

from __future__ import annotations

import argparse
import csv
import shutil
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


PARQUET_SCHEMA = pa.schema(
    [
        ("record_id", pa.int64()),
        ("product_id", pa.int64()),
        ("amount", pa.float64()),
        ("event_date", pa.string()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate large, tiny, zipped, malformed, and corrupt files for PySpark I/O labs."
    )
    parser.add_argument("--output-dir", default="data/io_performance")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--large-csv-rows", type=int, default=100_000)
    parser.add_argument(
        "--tiny-tables",
        nargs="+",
        default=["customers", "orders", "order_items"],
        help="Tables that receive one-row CSV files.",
    )
    parser.add_argument("--tiny-files-per-table", type=int, default=100)
    parser.add_argument("--lookup-table", default="products")
    parser.add_argument("--lookup-files", type=int, default=25)
    parser.add_argument("--parquet-files", type=int, default=2)
    parser.add_argument("--parquet-rows-per-file", type=int, default=100_000)
    parser.add_argument(
        "--allow-high-file-count",
        action="store_true",
        help="Required when the requested tiny-file total exceeds 5,000 files.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    numeric_values = {
        "large_csv_rows": args.large_csv_rows,
        "tiny_files_per_table": args.tiny_files_per_table,
        "lookup_files": args.lookup_files,
        "parquet_files": args.parquet_files,
        "parquet_rows_per_file": args.parquet_rows_per_file,
    }
    for name, value in numeric_values.items():
        if value <= 0:
            raise ValueError(f"{name} must be greater than zero.")

    tiny_file_total = len(args.tiny_tables) * args.tiny_files_per_table + args.lookup_files
    if tiny_file_total > 5_000 and not args.allow_high_file_count:
        raise ValueError(
            f"Requested {tiny_file_total:,} tiny files. "
            "Add --allow-high-file-count when this is intentional."
        )


def prepare_output_dir(path: Path, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"{path} exists. Use --overwrite to rebuild it.")
        shutil.rmtree(path)
    path.mkdir(parents=True)


def csv_row(row_num: int) -> list[object]:
    return [
        row_num,
        1_000 + (row_num % 500),
        round(5.0 + ((row_num * 17) % 50_000) / 100, 2),
        f"2026-01-{1 + (row_num % 28):02d}",
    ]


def write_large_csv(output_dir: Path, row_count: int) -> Path:
    output_dir.mkdir(parents=True)
    path = output_dir / "large_extract.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["record_id", "product_id", "amount", "event_date"])
        for row_num in range(1, row_count + 1):
            writer.writerow(csv_row(row_num))
    return path


def write_one_row_csv_files(output_dir: Path, table: str, file_count: int) -> None:
    table_dir = output_dir / table
    table_dir.mkdir(parents=True)
    for file_num in range(1, file_count + 1):
        path = table_dir / f"part_{file_num:05d}.csv"
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(["record_id", "product_id", "amount", "event_date"])
            writer.writerow(csv_row(file_num))


def parquet_table(start: int, row_count: int) -> pa.Table:
    record_ids = list(range(start, start + row_count))
    return pa.Table.from_arrays(
        [
            pa.array(record_ids, type=pa.int64()),
            pa.array((1_000 + (value % 500) for value in record_ids), type=pa.int64()),
            pa.array((5.0 + ((value * 17) % 50_000) / 100 for value in record_ids), type=pa.float64()),
            pa.array((f"2026-01-{1 + (value % 28):02d}" for value in record_ids), type=pa.string()),
        ],
        schema=PARQUET_SCHEMA,
    )


def write_many_parquets(output_dir: Path, file_count: int, rows_per_file: int) -> None:
    output_dir.mkdir(parents=True)
    for file_num in range(file_count):
        table = parquet_table(file_num * rows_per_file + 1, rows_per_file)
        pq.write_table(
            table,
            output_dir / f"part_{file_num + 1:05d}.parquet",
            compression="snappy",
        )


def write_bad_csv_fixtures(output_dir: Path) -> None:
    output_dir.mkdir(parents=True)
    (output_dir / "malformed_rows.csv").write_text(
        "record_id,product_id,amount,event_date\n"
        "1,1001,19.95,2026-01-01\n"
        "2,1002,not_a_number,2026-01-02\n"
        "3,1003,42.00\n"
        "4,1004,18.25,2026-01-04,unexpected_column\n"
        "broken,row\n",
        encoding="utf-8",
    )


def write_bad_parquet_fixtures(output_dir: Path) -> None:
    output_dir.mkdir(parents=True)
    valid_rows = pa.Table.from_pylist(
        [
            {"record_id": 1, "product_id": 1001, "amount": 19.95, "event_date": "2026-01-01"},
            {"record_id": 2, "product_id": 1002, "amount": -5.0, "event_date": "bad-date"},
            {"record_id": 3, "product_id": None, "amount": 30.0, "event_date": "2026-01-03"},
        ],
        schema=PARQUET_SCHEMA,
    )
    pq.write_table(valid_rows, output_dir / "logical_bad_rows.parquet", compression="snappy")
    (output_dir / "corrupt_file.parquet").write_bytes(b"not a parquet file\n")


def write_zip_fixture(output_dir: Path, source_csv: Path) -> None:
    output_dir.mkdir(parents=True)
    archive = output_dir / "incoming_extract.zip"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zip_handle:
        zip_handle.write(source_csv, arcname="incoming/large_extract.csv")


def main() -> None:
    args = parse_args()
    validate_args(args)
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, args.overwrite)

    large_csv = write_large_csv(output_dir / "large_csv", args.large_csv_rows)
    for table in args.tiny_tables:
        write_one_row_csv_files(output_dir / "one_row_csv", table, args.tiny_files_per_table)
    write_one_row_csv_files(output_dir / "one_row_csv", args.lookup_table, args.lookup_files)
    write_many_parquets(
        output_dir / "many_parquets",
        args.parquet_files,
        args.parquet_rows_per_file,
    )
    write_bad_csv_fixtures(output_dir / "bad_csv")
    write_bad_parquet_fixtures(output_dir / "bad_parquet")
    write_zip_fixture(output_dir / "zip", large_csv)

    tiny_total = len(args.tiny_tables) * args.tiny_files_per_table + args.lookup_files
    print(f"Generated I/O performance fixtures under {output_dir}")
    print(f"Tiny CSV files: {tiny_total:,}")
    print(f"Parquet rows: {args.parquet_files * args.parquet_rows_per_file:,}")


if __name__ == "__main__":
    main()
