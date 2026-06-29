#!/usr/bin/env python3
"""Generate CSV, JSON, and Parquet sources for PostgreSQL loading labs."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq


SCENARIOS = {
    "json-small-customer": ("01_json_small_customer", "json", ["customer"], "small"),
    "json-small-multi": (
        "02_json_small_multi",
        "json",
        ["customer", "sales", "product", "location"],
        "small",
    ),
    "json-large-sales": ("03_json_large_sales", "json", ["sales"], "large"),
    "csv-small-emp": ("04_csv_small_emp", "csv", ["emp"], "small"),
    "csv-small-multi": (
        "05_csv_small_multi",
        "csv",
        ["emp", "dept", "projects", "emp_projects"],
        "small",
    ),
    "csv-large-emp": ("06_csv_large_emp", "csv", ["emp"], "large"),
    "parquet-small-transaction": (
        "07_parquet_small_sales_transaction",
        "parquet",
        ["sales_transaction"],
        "small",
    ),
    "parquet-small-multi": (
        "08_parquet_small_multi",
        "parquet",
        ["customer", "sales", "product", "location"],
        "small",
    ),
    "parquet-large-sales": ("09_parquet_large_sales", "parquet", ["sales"], "large"),
}


COLUMNS = {
    "location": ["location_id", "location_name", "region"],
    "product": ["product_id", "product_name", "category", "unit_price", "active"],
    "customer": ["customer_id", "customer_name", "email", "location_id", "created_at"],
    "sales": [
        "sale_id",
        "customer_id",
        "product_id",
        "location_id",
        "quantity",
        "unit_price",
        "sale_ts",
    ],
    "dept": ["deptno", "dname", "loc"],
    "emp": ["empno", "ename", "job", "mgr", "hiredate", "sal", "commission", "deptno"],
    "projects": ["project_id", "project_name", "budget", "location_id"],
    "emp_projects": ["emp_project_id", "empno", "project_id", "start_date", "end_date"],
    "sales_transaction": [
        "transaction_id",
        "customer_id",
        "product_id",
        "location_id",
        "amount",
        "transaction_ts",
    ],
    "sales_transaction_changes": [
        "operation",
        "transaction_id",
        "amount",
        "transaction_ts",
        "change_ts",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default="data/database_sources")
    parser.add_argument(
        "--scenario",
        nargs="+",
        choices=["all", *SCENARIOS, "ultra", "cdc"],
        default=["all"],
    )
    parser.add_argument("--small-file-count", type=int, default=20)
    parser.add_argument("--small-rows-per-file", type=int, default=10)
    parser.add_argument("--large-file-count", type=int, default=2)
    parser.add_argument("--large-rows-per-file", type=int, default=100_000)
    parser.add_argument("--ultra-file-count", type=int, default=1_000_000)
    parser.add_argument("--ultra-rows-per-file", type=int, default=1)
    parser.add_argument("--ultra-format", choices=["csv", "json", "parquet"], default="json")
    parser.add_argument("--allow-ultra", action="store_true")
    parser.add_argument("--cdc-updates", type=int, default=100)
    parser.add_argument("--cdc-deletes", type=int, default=25)
    parser.add_argument("--cdc-rows-per-file", type=int, default=100)
    parser.add_argument("--overwrite", action="store_true")
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    names = [
        "small_file_count",
        "small_rows_per_file",
        "large_file_count",
        "large_rows_per_file",
        "ultra_file_count",
        "ultra_rows_per_file",
        "cdc_updates",
        "cdc_deletes",
        "cdc_rows_per_file",
    ]
    for name in names:
        value = getattr(args, name)
        if name in {"cdc_updates", "cdc_deletes"}:
            if value < 0:
                raise ValueError(f"{name} cannot be negative.")
        elif value <= 0:
            raise ValueError(f"{name} must be greater than zero.")
    if "ultra" in args.scenario and not args.allow_ultra:
        raise ValueError(
            "The ultra scenario can create up to one million files. "
            "Add --allow-ultra only when the filesystem and cleanup plan are ready."
        )


def prepare_output_dir(path: Path, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"{path} exists. Use --overwrite to rebuild it.")
        shutil.rmtree(path)
    path.mkdir(parents=True)
    (path / "_manifests").mkdir()


def iso_datetime(day_offset: int, second_offset: int = 0) -> str:
    value = datetime(2026, 1, 1, 8, 0, 0) + timedelta(days=day_offset, seconds=second_offset)
    return value.isoformat(sep=" ")


def row_for(table: str, row_num: int) -> dict[str, object]:
    location_id = 1 + ((row_num - 1) % 100)
    product_id = 1 + ((row_num - 1) % 1_000)
    customer_id = 1 + ((row_num - 1) % 100_000)
    deptno = (1 + ((row_num - 1) % 10)) * 10

    if table == "location":
        return {
            "location_id": row_num,
            "location_name": f"location_{row_num:06d}",
            "region": ["north", "south", "east", "west"][row_num % 4],
        }
    if table == "product":
        return {
            "product_id": row_num,
            "product_name": f"product_{row_num:08d}",
            "category": f"category_{row_num % 20:02d}",
            "unit_price": round(5 + ((row_num * 17) % 50_000) / 100, 2),
            "active": row_num % 20 != 0,
        }
    if table == "customer":
        return {
            "customer_id": row_num,
            "customer_name": f"customer_{row_num:09d}",
            "email": f"customer_{row_num:09d}@example.com",
            "location_id": location_id,
            "created_at": iso_datetime(row_num % 365),
        }
    if table == "sales":
        return {
            "sale_id": row_num,
            "customer_id": customer_id,
            "product_id": product_id,
            "location_id": location_id,
            "quantity": 1 + (row_num % 10),
            "unit_price": round(5 + ((row_num * 17) % 50_000) / 100, 2),
            "sale_ts": iso_datetime(row_num % 365, row_num % 86_400),
        }
    if table == "dept":
        return {
            "deptno": row_num * 10,
            "dname": f"department_{row_num:04d}",
            "loc": f"location_{location_id:03d}",
        }
    if table == "emp":
        return {
            "empno": 7_000 + row_num,
            "ename": f"employee_{row_num:09d}",
            "job": ["clerk", "analyst", "manager", "developer"][row_num % 4],
            "mgr": None if row_num <= 10 else 7_000 + ((row_num - 1) % 10) + 1,
            "hiredate": (date(2000, 1, 1) + timedelta(days=row_num % 9_000)).isoformat(),
            "sal": round(1_000 + ((row_num * 43) % 65_000), 2),
            "commission": None if row_num % 4 else round(100 + ((row_num * 7) % 900), 2),
            "deptno": deptno,
        }
    if table == "projects":
        return {
            "project_id": row_num,
            "project_name": f"project_{row_num:07d}",
            "budget": round(50_000 + ((row_num * 997) % 5_000_000), 2),
            "location_id": location_id,
        }
    if table == "emp_projects":
        return {
            "emp_project_id": row_num,
            "empno": 7_000 + customer_id,
            "project_id": product_id,
            "start_date": (date(2020, 1, 1) + timedelta(days=row_num % 2_000)).isoformat(),
            "end_date": None
            if row_num % 5
            else (date(2020, 1, 1) + timedelta(days=(row_num % 2_000) + 180)).isoformat(),
        }
    if table == "sales_transaction":
        return {
            "transaction_id": row_num,
            "customer_id": customer_id,
            "product_id": product_id,
            "location_id": location_id,
            "amount": round(5 + ((row_num * 31) % 250_000) / 100, 2),
            "transaction_ts": iso_datetime(row_num % 365, row_num % 86_400),
        }
    raise ValueError(f"Unsupported table: {table}")


def cdc_row(row_num: int, update_count: int) -> dict[str, object]:
    is_update = row_num <= update_count
    return {
        "operation": "U" if is_update else "D",
        "transaction_id": row_num,
        "amount": round(100 + ((row_num * 29) % 100_000) / 100, 2) if is_update else None,
        "transaction_ts": iso_datetime(row_num % 365, row_num % 86_400) if is_update else None,
        "change_ts": iso_datetime(400, row_num % 86_400),
    }


def write_rows(path: Path, fmt: str, table: str, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS[table])
            writer.writeheader()
            writer.writerows(rows)
        return
    if fmt == "json":
        with path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, separators=(",", ":")))
                handle.write("\n")
        return
    if fmt == "parquet":
        pq.write_table(pa.Table.from_pylist(rows), path, compression="snappy")
        return
    raise ValueError(fmt)


def generate_table(
    root: Path,
    fmt: str,
    table: str,
    file_count: int,
    rows_per_file: int,
    row_factory: Callable[[int], dict[str, object]] | None = None,
) -> dict[str, object]:
    extension = "json" if fmt == "json" else fmt
    factory = row_factory or (lambda row_num: row_for(table, row_num))
    for file_num in range(file_count):
        start = file_num * rows_per_file + 1
        rows = [factory(row_num) for row_num in range(start, start + rows_per_file)]
        write_rows(root / table / f"part_{file_num + 1:07d}.{extension}", fmt, table, rows)
        if file_count >= 100_000 and (file_num + 1) % 10_000 == 0:
            print(f"{table}: wrote {file_num + 1:,} of {file_count:,} files")
    return {"table": table, "format": fmt, "files": file_count, "rows": file_count * rows_per_file}


def write_manifest(output_dir: Path, scenario: str, details: list[dict[str, object]]) -> None:
    payload = {"scenario": scenario, "tables": details}
    path = output_dir / "_manifests" / f"{scenario}.json"
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def generate_named_scenario(output_dir: Path, scenario: str, args: argparse.Namespace) -> None:
    folder, fmt, tables, size = SCENARIOS[scenario]
    if size == "small":
        file_count, rows_per_file = args.small_file_count, args.small_rows_per_file
    else:
        file_count, rows_per_file = args.large_file_count, args.large_rows_per_file
    details = [
        generate_table(output_dir / folder, fmt, table, file_count, rows_per_file)
        for table in tables
    ]
    write_manifest(output_dir, scenario, details)


def generate_ultra(output_dir: Path, args: argparse.Namespace) -> None:
    details = [
        generate_table(
            output_dir / "10_ultra_sales_transaction",
            args.ultra_format,
            "sales_transaction",
            args.ultra_file_count,
            args.ultra_rows_per_file,
        )
    ]
    write_manifest(output_dir, "ultra", details)


def generate_cdc(output_dir: Path, args: argparse.Namespace) -> None:
    total = args.cdc_updates + args.cdc_deletes
    if total == 0:
        return
    file_count = (total + args.cdc_rows_per_file - 1) // args.cdc_rows_per_file
    details: list[dict[str, object]] = []
    for fmt in ("csv", "json", "parquet"):
        extension = "json" if fmt == "json" else fmt
        for file_num in range(file_count):
            start = file_num * args.cdc_rows_per_file + 1
            stop = min(start + args.cdc_rows_per_file, total + 1)
            rows = [cdc_row(row_num, args.cdc_updates) for row_num in range(start, stop)]
            write_rows(
                output_dir
                / "11_cdc_sales_transaction"
                / fmt
                / "sales_transaction_changes"
                / f"part_{file_num + 1:07d}.{extension}",
                fmt,
                "sales_transaction_changes",
                rows,
            )
        details.append({"table": "sales_transaction_changes", "format": fmt, "files": file_count, "rows": total})
    write_manifest(output_dir, "cdc", details)


def main() -> None:
    args = parse_args()
    validate_args(args)
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, args.overwrite)

    requested = set(args.scenario)
    if "all" in requested:
        requested = set(SCENARIOS) | {"cdc"}
    for scenario in SCENARIOS:
        if scenario in requested:
            print(f"Generating {scenario}")
            generate_named_scenario(output_dir, scenario, args)
    if "ultra" in requested:
        print("Generating ultra file-count scenario")
        generate_ultra(output_dir, args)
    if "cdc" in requested:
        print("Generating update/delete change files")
        generate_cdc(output_dir, args)

    print(f"Generated database tutorial sources under {output_dir}")


if __name__ == "__main__":
    main()
