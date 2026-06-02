#!/usr/bin/env python3
"""Generate small and large employee update files in CSV, JSON, and Parquet."""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


UPDATE_SCHEMA = pa.schema(
    [
        ("empno", pa.int64()),
        ("new_job", pa.string()),
        ("new_sal", pa.float64()),
        ("new_commission", pa.float64()),
        ("update_reason", pa.string()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate employee update files for performance demonstrations.")
    parser.add_argument("--output-dir", default="data/update_records")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--small-records", type=int, default=100)
    parser.add_argument("--large-records", type=int, default=100_000)
    return parser.parse_args()


def prepare_output_dir(output_dir: Path, overwrite: bool) -> None:
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(f"{output_dir} already exists. Use --overwrite to rebuild it.")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)


def update_row(row_num: int, label: str) -> dict[str, object]:
    return {
        "empno": 7000 + row_num,
        "new_job": "senior_" + ["clerk", "salesman", "analyst", "manager", "developer"][row_num % 5],
        "new_sal": float(1_000 + ((row_num * 43) % 65_000)),
        "new_commission": None if row_num % 4 else float(100 + ((row_num * 7) % 900)),
        "update_reason": f"{label}_salary_job_adjustment",
    }


def rows(count: int, label: str) -> list[dict[str, object]]:
    return [update_row(row_num, label) for row_num in range(1, count + 1)]


def write_csv(path: Path, records: list[dict[str, object]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0].keys()))
        writer.writeheader()
        writer.writerows(records)


def write_json(path: Path, records: list[dict[str, object]]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, separators=(",", ":")))
            handle.write("\n")


def write_parquet(path: Path, records: list[dict[str, object]]) -> None:
    table = pa.Table.from_pylist(records, schema=UPDATE_SCHEMA)
    pq.write_table(table, path, compression="snappy")


def write_all_formats(output_dir: Path, label: str, count: int) -> None:
    records = rows(count, label)
    label_dir = output_dir / label
    label_dir.mkdir(parents=True)
    write_csv(label_dir / "emp_updates.csv", records)
    write_json(label_dir / "emp_updates.json", records)
    write_parquet(label_dir / "emp_updates.parquet", records)


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, args.overwrite)

    write_all_formats(output_dir, "small", args.small_records)
    write_all_formats(output_dir, "large", args.large_records)

    print(f"Wrote update files to {output_dir}")


if __name__ == "__main__":
    main()
