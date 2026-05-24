#!/usr/bin/env python3
"""Generate partitioned CSV training data for PySpark examples."""

from __future__ import annotations

import argparse
import csv
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterable


DEPT_NAMES = [
    "accounting",
    "research",
    "sales",
    "operations",
    "finance",
    "marketing",
    "engineering",
    "support",
    "hr",
    "logistics",
]

LOCATIONS = [
    "new_york",
    "dallas",
    "chicago",
    "boston",
    "atlanta",
    "denver",
    "seattle",
    "austin",
    "phoenix",
    "miami",
]

EMP_NAMES = [
    "adam",
    "bella",
    "carl",
    "divya",
    "eli",
    "fatima",
    "gita",
    "hugo",
    "irene",
    "jay",
]

JOBS = [
    "clerk",
    "salesman",
    "analyst",
    "manager",
    "developer",
    "tester",
    "architect",
    "consultant",
]

PROJECT_NAMES = [
    "Pulse",
    "Streamline",
    "SwiftSync",
    "Flux",
    "Atlas",
    "Beacon",
    "Cobalt",
    "Delta",
    "Echo",
    "Fusion",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build partitioned CSV files for dept, emp, projects, emp_projects, and salgrade."
    )
    parser.add_argument("--output-dir", default="data/generated_csv", help="Directory to write CSV data into.")
    parser.add_argument("--overwrite", action="store_true", help="Delete the output directory before writing.")
    parser.add_argument("--dept-records", type=int, default=100)
    parser.add_argument("--dept-rows-per-file", type=int, default=10)
    parser.add_argument("--emp-records", type=int, default=1_000_000)
    parser.add_argument("--emp-rows-per-file", type=int, default=1_000)
    parser.add_argument("--project-records", type=int, default=100)
    parser.add_argument("--project-rows-per-file", type=int, default=100)
    parser.add_argument(
        "--emp-projects-rows-per-file",
        type=int,
        default=100_000,
        help="Rows per emp_projects part. Defaults to 100,000 to avoid creating 100,000 tiny files.",
    )
    parser.add_argument("--salgrade-records", type=int, default=1_000_000)
    parser.add_argument("--salgrade-rows-per-file", type=int, default=1_000)
    parser.add_argument(
        "--skip-emp-projects",
        action="store_true",
        help="Skip the large emp_projects cross-product table.",
    )
    return parser.parse_args()


def validate_positive(name: str, value: int) -> None:
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero.")


def prepare_output_dir(output_dir: Path, overwrite: bool) -> None:
    if output_dir.exists():
        if not overwrite:
            raise FileExistsError(f"{output_dir} already exists. Use --overwrite to rebuild it.")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)


def write_partitioned_csv(
    output_dir: Path,
    prefix: str,
    header: list[str],
    total_rows: int,
    rows_per_file: int,
    row_factory: Callable[[int], Iterable[object]],
) -> None:
    validate_positive(f"{prefix} total_rows", total_rows)
    validate_positive(f"{prefix} rows_per_file", rows_per_file)

    part = 1
    rows_written_in_part = 0
    handle = None
    writer = None

    try:
        for row_num in range(1, total_rows + 1):
            if writer is None or rows_written_in_part == rows_per_file:
                if handle:
                    handle.close()
                path = output_dir / f"{prefix}_part_{part:04d}.csv"
                handle = path.open("w", newline="", encoding="utf-8")
                writer = csv.writer(handle)
                writer.writerow(header)
                part += 1
                rows_written_in_part = 0

            writer.writerow(row_factory(row_num))
            rows_written_in_part += 1
    finally:
        if handle:
            handle.close()


def dept_row(row_num: int) -> list[object]:
    deptno = row_num * 10
    return [
        deptno,
        f"{DEPT_NAMES[(row_num - 1) % len(DEPT_NAMES)]}_{row_num:03d}",
        LOCATIONS[(row_num - 1) % len(LOCATIONS)],
    ]


def emp_row(row_num: int, dept_records: int) -> list[object]:
    empno = 7000 + row_num
    mgr = "" if row_num <= 10 else 7000 + ((row_num - 1) // 10)
    hiredate = date(1988, 1, 1) + timedelta(days=(row_num - 1) % 13_000)
    sal = 700 + ((row_num * 37) % 49_300)
    commission = "" if row_num % 3 else 50 + ((row_num * 11) % 950)
    deptno = (((row_num - 1) % dept_records) + 1) * 10
    return [
        empno,
        f"{EMP_NAMES[(row_num - 1) % len(EMP_NAMES)]}_{row_num}",
        JOBS[(row_num - 1) % len(JOBS)],
        mgr,
        hiredate.isoformat(),
        f"{sal:.2f}",
        commission,
        deptno,
    ]


def project_row(row_num: int) -> list[object]:
    return [
        row_num,
        f"{PROJECT_NAMES[(row_num - 1) % len(PROJECT_NAMES)]}_{row_num:03d}",
        row_num * 10_000,
        row_num * 100,
    ]


def emp_project_row(row_num: int, project_records: int) -> list[object]:
    emp_index = ((row_num - 1) // project_records) + 1
    projectno = ((row_num - 1) % project_records) + 1
    start_date = date(1989, 1, 1) + timedelta(days=(emp_index + projectno) % 9_000)
    end_date = "" if projectno % 10 == 0 else (start_date + timedelta(days=365 + (projectno % 90))).isoformat()
    return [
        1000 + row_num,
        7000 + emp_index,
        projectno,
        start_date.isoformat(),
        end_date,
    ]


def salgrade_row(row_num: int) -> list[object]:
    losal = 700 + ((row_num - 1) * 50)
    hisal = losal + 49
    return [row_num, losal, hisal]


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, args.overwrite)

    write_partitioned_csv(
        output_dir,
        "dept",
        ["deptno", "dname", "loc"],
        args.dept_records,
        args.dept_rows_per_file,
        dept_row,
    )
    write_partitioned_csv(
        output_dir,
        "emp",
        ["empno", "ename", "job", "mgr", "hiredate", "sal", "commission", "deptno"],
        args.emp_records,
        args.emp_rows_per_file,
        lambda row_num: emp_row(row_num, args.dept_records),
    )
    write_partitioned_csv(
        output_dir,
        "projects",
        ["projectno", "project_name", "budget", "monthly_commission"],
        args.project_records,
        args.project_rows_per_file,
        project_row,
    )
    if not args.skip_emp_projects:
        write_partitioned_csv(
            output_dir,
            "emp_projects",
            ["emp_projectno", "empno", "projectno", "start_date", "end_date"],
            args.emp_records * args.project_records,
            args.emp_projects_rows_per_file,
            lambda row_num: emp_project_row(row_num, args.project_records),
        )
    write_partitioned_csv(
        output_dir,
        "salgrade",
        ["grade", "losal", "hisal"],
        args.salgrade_records,
        args.salgrade_rows_per_file,
        salgrade_row,
    )

    print(f"Wrote CSV files to {output_dir}")


if __name__ == "__main__":
    main()
