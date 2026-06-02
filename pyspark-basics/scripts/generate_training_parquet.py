#!/usr/bin/env python3
"""Generate partitioned Parquet training data for PySpark examples."""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path
from typing import Callable

import pyarrow as pa
import pyarrow.parquet as pq

from generate_training_csv import (
    dept_row,
    emp_project_row,
    emp_row,
    project_row,
    salgrade_row,
)


SCHEMAS = {
    "dept": pa.schema(
        [
            ("deptno", pa.int64()),
            ("dname", pa.string()),
            ("loc", pa.string()),
        ]
    ),
    "emp": pa.schema(
        [
            ("empno", pa.int64()),
            ("ename", pa.string()),
            ("job", pa.string()),
            ("mgr", pa.int64()),
            ("hiredate", pa.string()),
            ("sal", pa.float64()),
            ("commission", pa.float64()),
            ("deptno", pa.int64()),
        ]
    ),
    "projects": pa.schema(
        [
            ("projectno", pa.int64()),
            ("project_name", pa.string()),
            ("budget", pa.int64()),
            ("monthly_commission", pa.int64()),
        ]
    ),
    "emp_projects": pa.schema(
        [
            ("emp_projectno", pa.int64()),
            ("empno", pa.int64()),
            ("projectno", pa.int64()),
            ("start_date", pa.string()),
            ("end_date", pa.string()),
        ]
    ),
    "salgrade": pa.schema(
        [
            ("grade", pa.int64()),
            ("losal", pa.int64()),
            ("hisal", pa.int64()),
        ]
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build partitioned Parquet files for dept, emp, projects, emp_projects, and salgrade."
    )
    parser.add_argument("--output-dir", default="data/generated_parquet_10000")
    parser.add_argument("--overwrite", action="store_true", help="Delete the output directory before writing.")
    parser.add_argument("--dept-records", type=int, default=100)
    parser.add_argument("--emp-records", type=int, default=1_000_000)
    parser.add_argument("--project-records", type=int, default=100)
    parser.add_argument("--salgrade-records", type=int, default=1_000_000)
    parser.add_argument(
        "--rows-per-file",
        type=int,
        default=10_000,
        help="Rows per Parquet file for every table.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=250_000,
        help="Safety limit. Use a larger value only when intentionally creating many tiny files.",
    )
    parser.add_argument(
        "--skip-emp-projects",
        action="store_true",
        help="Skip the emp_projects cross-product table.",
    )
    return parser.parse_args()


def prepare_output_dir(output_dir: Path, overwrite: bool) -> None:
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(f"{output_dir} already exists. Use --overwrite to rebuild it.")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)


def clean_emp(row: list[object]) -> dict[str, object]:
    mgr = row[3]
    commission = row[6]
    return {
        "empno": row[0],
        "ename": row[1],
        "job": row[2],
        "mgr": None if mgr == "" else mgr,
        "hiredate": row[4],
        "sal": float(row[5]),
        "commission": None if commission == "" else float(commission),
        "deptno": row[7],
    }


def clean_emp_project(row: list[object]) -> dict[str, object]:
    return {
        "emp_projectno": row[0],
        "empno": row[1],
        "projectno": row[2],
        "start_date": row[3],
        "end_date": None if row[4] == "" else row[4],
    }


def dict_from_row(header: list[str], row: list[object]) -> dict[str, object]:
    return dict(zip(header, row))


def write_partitioned_parquet(
    output_dir: Path,
    table_name: str,
    total_rows: int,
    rows_per_file: int,
    row_factory: Callable[[int], dict[str, object]],
    max_files: int,
) -> None:
    if total_rows <= 0:
        raise ValueError(f"{table_name} total rows must be greater than zero.")
    if rows_per_file <= 0:
        raise ValueError(f"{table_name} rows per file must be greater than zero.")

    file_count = (total_rows + rows_per_file - 1) // rows_per_file
    if file_count > max_files:
        raise ValueError(
            f"{table_name} would create {file_count:,} files. "
            f"Increase --max-files if you really want to create that many tiny files."
        )

    table_dir = output_dir / table_name
    table_dir.mkdir(parents=True, exist_ok=True)
    schema = SCHEMAS[table_name]

    row_num = 1
    for part in range(1, file_count + 1):
        batch_size = min(rows_per_file, total_rows - row_num + 1)
        rows = [row_factory(current) for current in range(row_num, row_num + batch_size)]
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, table_dir / f"{table_name}_part_{part:04d}.parquet", compression="snappy")
        row_num += batch_size


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, args.overwrite)

    write_partitioned_parquet(
        output_dir,
        "dept",
        args.dept_records,
        args.rows_per_file,
        lambda row_num: dict_from_row(["deptno", "dname", "loc"], dept_row(row_num)),
        args.max_files,
    )
    write_partitioned_parquet(
        output_dir,
        "emp",
        args.emp_records,
        args.rows_per_file,
        lambda row_num: clean_emp(emp_row(row_num, args.dept_records)),
        args.max_files,
    )
    write_partitioned_parquet(
        output_dir,
        "projects",
        args.project_records,
        args.rows_per_file,
        lambda row_num: dict_from_row(
            ["projectno", "project_name", "budget", "monthly_commission"],
            project_row(row_num),
        ),
        args.max_files,
    )
    if not args.skip_emp_projects:
        write_partitioned_parquet(
            output_dir,
            "emp_projects",
            args.emp_records * args.project_records,
            args.rows_per_file,
            lambda row_num: clean_emp_project(emp_project_row(row_num, args.project_records)),
            args.max_files,
        )
    write_partitioned_parquet(
        output_dir,
        "salgrade",
        args.salgrade_records,
        args.rows_per_file,
        salgrade_row_dict,
        args.max_files,
    )

    print(f"Wrote Parquet files to {output_dir}")


def salgrade_row_dict(row_num: int) -> dict[str, object]:
    return dict_from_row(["grade", "losal", "hisal"], salgrade_row(row_num))


if __name__ == "__main__":
    main()
