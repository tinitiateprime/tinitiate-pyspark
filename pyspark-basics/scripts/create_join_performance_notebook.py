#!/usr/bin/env python3
"""Create a professional notebook for small-file versus larger-file join performance."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path


def md(source: str) -> dict:
    cleaned = textwrap.dedent(source).strip()
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": [line + "\n" for line in cleaned.splitlines()],
    }


def code(source: str) -> dict:
    cleaned = textwrap.dedent(source).strip()
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" for line in cleaned.splitlines()],
    }


cells = [
    md(
        """
        # Join Performance: Small Files vs Larger Files

        This notebook demonstrates how file layout affects Spark join performance. It compares the same logical data stored as many tiny files versus fewer larger files, then measures join runtimes in a repeatable way.

        Key idea: Spark is distributed, but every input file still has planning, listing, metadata, and task scheduling overhead. A large number of tiny files can slow down a simple join before the actual data processing becomes the bottleneck.
        """
    ),
    md(
        """
        ## Datasets Used

        The notebook expects the generated folders already created in this repo:

        - `data/generated_csv_10`: CSV, 10 rows per file for `dept`, `emp`, `projects`, and `salgrade`
        - `data/generated_csv_10000`: CSV, 10,000 rows per file, including `emp_projects`
        - `data/generated_json_10`: 10 rows per file for `dept`, `emp`, `projects`, and `salgrade`
        - `data/generated_json_10000`: 10,000 rows per file, including `emp_projects`
        - `data/generated_parquet_10`: 10 rows per file for `dept`, `emp`, `projects`, and `salgrade`
        - `data/generated_parquet_10000`: 10,000 rows per file, including `emp_projects`

        The comparisons are grouped by format: CSV vs CSV, JSON vs JSON, and Parquet vs Parquet.

        `emp_projects` is intentionally skipped in all 10-row tiny-file folders because the full cross product would create 10,000,000 tiny files per format.
        """
    ),
    code(
        """
        from pathlib import Path
        import gc
        import html
        import shutil
        import time

        from IPython.display import HTML, display
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        spark = (
            SparkSession.builder
            .appName("join-performance-small-vs-large-files")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("WARN")

        DATA_ROOT = Path.cwd() / "data"
        UPDATE_ROOT = DATA_ROOT / "update_records"
        UPDATE_OUTPUT_ROOT = DATA_ROOT / "update_benchmark_output"
        print(f"Data root: {DATA_ROOT}")
        print(f"Spark version: {spark.version}")
        """
    ),
    code(
        """
        scenarios = [
            {
                "name": "CSV tiny files",
                "short": "csv_10",
                "format": "csv",
                "base": DATA_ROOT / "generated_csv_10",
                "rows_per_file": 10,
                "layout": "10 rows/file",
                "has_emp_projects": False,
            },
            {
                "name": "CSV larger files",
                "short": "csv_10000",
                "format": "csv",
                "base": DATA_ROOT / "generated_csv_10000",
                "rows_per_file": 10_000,
                "layout": "10,000 rows/file",
                "has_emp_projects": True,
            },
            {
                "name": "JSON tiny files",
                "short": "json_10",
                "format": "json",
                "base": DATA_ROOT / "generated_json_10",
                "rows_per_file": 10,
                "layout": "10 rows/file",
                "has_emp_projects": False,
            },
            {
                "name": "JSON larger files",
                "short": "json_10000",
                "format": "json",
                "base": DATA_ROOT / "generated_json_10000",
                "rows_per_file": 10_000,
                "layout": "10,000 rows/file",
                "has_emp_projects": True,
            },
            {
                "name": "Parquet tiny files",
                "short": "parquet_10",
                "format": "parquet",
                "base": DATA_ROOT / "generated_parquet_10",
                "rows_per_file": 10,
                "layout": "10 rows/file",
                "has_emp_projects": False,
            },
            {
                "name": "Parquet larger files",
                "short": "parquet_10000",
                "format": "parquet",
                "base": DATA_ROOT / "generated_parquet_10000",
                "rows_per_file": 10_000,
                "layout": "10,000 rows/file",
                "has_emp_projects": True,
            },
        ]

        format_pairs = {
            "csv": ["csv_10", "csv_10000"],
            "json": ["json_10", "json_10000"],
            "parquet": ["parquet_10", "parquet_10000"],
        }

        tables = ["dept", "emp", "projects", "emp_projects", "salgrade"]
        """
    ),
    code(
        """
        def table_path(scenario, table):
            base = Path(scenario["base"])
            fmt = scenario["format"]
            if fmt in {"parquet", "json"}:
                return base / table
            if fmt == "csv":
                return str(base / f"{table}_part_*.csv")
            raise ValueError(fmt)


        def load_table(scenario, table):
            fmt = scenario["format"]
            path = table_path(scenario, table)
            if fmt == "parquet":
                return spark.read.parquet(str(path))
            if fmt == "json":
                return spark.read.json(str(path))
            if fmt == "csv":
                return spark.read.option("header", True).option("inferSchema", True).csv(str(path))
            raise ValueError(fmt)


        def folder_stats(scenario, table):
            fmt = scenario["format"]
            base = Path(scenario["base"])
            if table == "emp_projects" and not scenario["has_emp_projects"]:
                return {
                    "scenario": scenario["name"],
                    "short": scenario["short"],
                    "table": table,
                    "format": fmt,
                    "layout": scenario["layout"],
                    "files": 0,
                    "mb": 0.0,
                    "exists": False,
                }

            if fmt in {"parquet", "json"}:
                folder = base / table
                pattern = f"*.{fmt}"
                files = list(folder.glob(pattern)) if folder.exists() else []
            else:
                files = list(base.glob(f"{table}_part_*.csv")) if base.exists() else []

            total_bytes = sum(path.stat().st_size for path in files)
            return {
                "scenario": scenario["name"],
                "short": scenario["short"],
                "table": table,
                "format": fmt,
                "layout": scenario["layout"],
                "files": len(files),
                "mb": total_bytes / 1024 / 1024,
                "exists": bool(files),
            }


        stats = [folder_stats(scenario, table) for scenario in scenarios for table in tables]
        stats
        """
    ),
    code(
        """
        def html_table(rows, columns):
            header = "".join(f"<th>{html.escape(col)}</th>" for col in columns)
            body = []
            for row in rows:
                body.append("<tr>" + "".join(f"<td>{html.escape(str(row.get(col, '')))}</td>" for col in columns) + "</tr>")
            return HTML(
                \"\"\"
                <style>
                .perf-table { border-collapse: collapse; font-family: Arial, sans-serif; font-size: 13px; }
                .perf-table th, .perf-table td { border: 1px solid #ddd; padding: 6px 9px; text-align: right; }
                .perf-table th:first-child, .perf-table td:first-child,
                .perf-table th:nth-child(2), .perf-table td:nth-child(2) { text-align: left; }
                .perf-table th { background: #243447; color: white; }
                .perf-table tr:nth-child(even) { background: #f7f9fb; }
                </style>
                \"\"\" + f"<table class='perf-table'><thead><tr>{header}</tr></thead><tbody>{''.join(body)}</tbody></table>"
            )


        def bar_chart(rows, label_key, value_key, title, color="#2563eb", suffix="", width=760):
            present = [row for row in rows if row.get(value_key, 0) is not None]
            max_value = max((float(row[value_key]) for row in present), default=1.0)
            parts = [
                "<div style='font-family:Arial,sans-serif;max-width:%dpx'>" % width,
                f"<h3 style='margin:8px 0 10px'>{html.escape(title)}</h3>",
            ]
            for row in present:
                value = float(row[value_key])
                pct = 0 if max_value == 0 else max(2, value / max_value * 100)
                label = html.escape(str(row[label_key]))
                shown = f"{value:,.2f}{suffix}" if isinstance(value, float) and value % 1 else f"{int(value):,}{suffix}"
                parts.append(
                    "<div style='display:grid;grid-template-columns:190px 1fr 110px;gap:8px;align-items:center;margin:6px 0'>"
                    f"<div style='font-size:13px'>{label}</div>"
                    "<div style='height:22px;background:#edf2f7;border-radius:4px;overflow:hidden'>"
                    f"<div style='height:22px;width:{pct:.1f}%;background:{color}'></div>"
                    "</div>"
                    f"<div style='font-size:13px;text-align:right'>{shown}</div>"
                    "</div>"
                )
            parts.append("</div>")
            return HTML("".join(parts))


        def comparison_card(title, left, right, metric_key, metric_label, suffix="", color="#2563eb"):
            values = [float(left[metric_key]), float(right[metric_key])]
            max_value = max(values) or 1.0

            def row_html(row):
                value = float(row[metric_key])
                pct = max(2, value / max_value * 100)
                shown = f"{value:,.2f}{suffix}" if value % 1 else f"{int(value):,}{suffix}"
                return (
                    "<div style='display:grid;grid-template-columns:160px 1fr 110px;gap:8px;align-items:center;margin:8px 0'>"
                    f"<div>{html.escape(row['layout'])}</div>"
                    "<div style='height:24px;background:#edf2f7;border-radius:4px;overflow:hidden'>"
                    f"<div style='height:24px;width:{pct:.1f}%;background:{color}'></div>"
                    "</div>"
                    f"<div style='text-align:right'>{shown}</div>"
                    "</div>"
                )

            ratio = float(left[metric_key]) / float(right[metric_key]) if float(right[metric_key]) else 0
            return HTML(
                "<div style='font-family:Arial,sans-serif;border:1px solid #d7dee8;border-radius:8px;padding:14px;margin:12px 0;max-width:820px'>"
                f"<h3 style='margin:0 0 8px'>{html.escape(title)}</h3>"
                f"<div style='font-size:13px;color:#4b5563;margin-bottom:8px'>{html.escape(metric_label)}</div>"
                f"{row_html(left)}{row_html(right)}"
                f"<div style='font-size:13px;color:#374151;margin-top:8px'>Tiny-file value is <b>{ratio:,.1f}x</b> the larger-file value.</div>"
                "</div>"
            )


        def rows_for_pair(rows, fmt):
            wanted = set(format_pairs[fmt])
            return [row for row in rows if row.get("short") in wanted]


        def rows_for_format_and_layout(rows, fmt, layout):
            return [row for row in rows if row.get("format") == fmt and row.get("layout") == layout]
        """
    ),
    code(
        """
        summary_rows = []
        for item in stats:
            summary_rows.append({
                "scenario": item["scenario"],
                "format": item["format"],
                "layout": item["layout"],
                "table": item["table"],
                "files": item["files"],
                "size_mb": round(item["mb"], 2),
            })

        display(html_table(summary_rows, ["format", "layout", "scenario", "table", "files", "size_mb"]))
        """
    ),
    md(
        """
        ## File Layout Comparison by Format

        These charts compare each format against itself. This keeps the analysis focused on file layout instead of mixing storage-format differences:

        - CSV tiny files vs CSV larger files
        - JSON tiny files vs JSON larger files
        - Parquet tiny files vs Parquet larger files
        """
    ),
    code(
        """
        for fmt in ["csv", "json", "parquet"]:
            emp_rows = [
                {
                    "short": item["short"],
                    "layout": item["layout"],
                    "files": item["files"],
                    "size_mb": round(item["mb"], 2),
                }
                for item in stats
                if item["format"] == fmt and item["table"] == "emp" and item["exists"]
            ]
            tiny, larger = emp_rows
            display(comparison_card(f"{fmt.upper()}: Employee file count", tiny, larger, "files", "Same 1,000,000 employees, different number of files", color="#7c3aed"))
            display(comparison_card(f"{fmt.upper()}: Employee data size", tiny, larger, "size_mb", "Data size can change slightly because each file has its own metadata/header overhead", suffix=" MB", color="#0891b2"))
        """
    ),
    md(
        """
        ## Benchmark 1: Read `emp` and Join to `dept`

        This join is intentionally simple:

        ```python
        emp.join(broadcast(dept), "deptno").count()
        ```

        The `dept` table is tiny, so the join logic is intentionally simple. When the tiny-file scenarios are slower, the difference is usually driven by file listing, scan planning, metadata handling, and task overhead rather than business logic complexity.
        """
    ),
    code(
        """
        def timed(label, fn):
            start = time.perf_counter()
            value = fn()
            elapsed = time.perf_counter() - start
            print(f"{label}: {elapsed:,.2f}s -> {value:,}")
            return elapsed, value


        def benchmark_emp_dept_join(scenario):
            spark.catalog.clearCache()
            gc.collect()
            emp = load_table(scenario, "emp")
            dept = load_table(scenario, "dept")
            joined = emp.join(F.broadcast(dept), "deptno", "inner")
            return timed(scenario["name"], lambda: joined.count())


        join_results = []
        for scenario in scenarios:
            if not Path(scenario["base"]).exists():
                print(f"Skipping missing folder: {scenario['base']}")
                continue
            elapsed, rows = benchmark_emp_dept_join(scenario)
            emp_files = next(item["files"] for item in stats if item["scenario"] == scenario["name"] and item["table"] == "emp")
            join_results.append({
                "scenario": scenario["name"],
                "short": scenario["short"],
                "format": scenario["format"],
                "layout": scenario["layout"],
                "rows_per_file": scenario["rows_per_file"],
                "emp_files": emp_files,
                "join_rows": rows,
                "seconds": round(elapsed, 2),
            })

        display(html_table(join_results, ["format", "layout", "scenario", "emp_files", "join_rows", "seconds"]))
        """
    ),
    code(
        """
        for fmt in ["csv", "json", "parquet"]:
            pair = rows_for_pair(join_results, fmt)
            if len(pair) == 2:
                tiny, larger = pair
                display(comparison_card(f"{fmt.upper()}: Join runtime", tiny, larger, "seconds", "emp JOIN dept runtime", suffix=" sec", color="#dc2626"))
                display(comparison_card(f"{fmt.upper()}: Files planned for emp", tiny, larger, "emp_files", "Input file count Spark has to plan before the join", color="#f59e0b"))
        """
    ),
    md(
        """
        ## Benchmark 2: Small Updates vs Large Updates

        CSV, JSON, and plain Parquet files are immutable file-based datasets. They do not support efficient row-level updates by themselves. A typical update pattern is:

        1. Read the base dataset.
        2. Read the update records.
        3. Join base records to update records by key.
        4. Replace changed columns.
        5. Write a new version of the dataset.

        This benchmark compares two update sizes for each format and layout:

        - Small update: 100 employee records.
        - Large update: 100,000 employee records.

        The important point is that even a small update may require scanning and rewriting the full `emp` dataset when using plain files.
        """
    ),
    code(
        """
        RUN_UPDATE_BENCHMARKS = True

        update_cases = [
            {"name": "Small update", "short": "small", "records": 100},
            {"name": "Large update", "short": "large", "records": 100_000},
        ]


        def load_updates(fmt, update_case):
            base = UPDATE_ROOT / update_case["short"]
            if fmt == "csv":
                return spark.read.option("header", True).option("inferSchema", True).csv(str(base / "emp_updates.csv"))
            if fmt == "json":
                return spark.read.json(str(base / "emp_updates.json"))
            if fmt == "parquet":
                return spark.read.parquet(str(base / "emp_updates.parquet"))
            raise ValueError(fmt)


        def write_by_format(df, fmt, output_path):
            writer = df.write.mode("overwrite")
            if fmt == "csv":
                writer.option("header", True).csv(str(output_path))
            elif fmt == "json":
                writer.json(str(output_path))
            elif fmt == "parquet":
                writer.parquet(str(output_path))
            else:
                raise ValueError(fmt)


        def benchmark_emp_updates(scenario, update_case):
            spark.catalog.clearCache()
            gc.collect()

            fmt = scenario["format"]
            emp = load_table(scenario, "emp")
            updates = load_updates(fmt, update_case)

            updated = (
                emp.alias("e")
                .join(F.broadcast(updates).alias("u"), "empno", "left")
                .select(
                    F.col("empno"),
                    F.col("ename"),
                    F.coalesce(F.col("new_job"), F.col("job")).alias("job"),
                    F.col("mgr"),
                    F.col("hiredate"),
                    F.coalesce(F.col("new_sal"), F.col("sal")).alias("sal"),
                    F.coalesce(F.col("new_commission"), F.col("commission")).alias("commission"),
                    F.col("deptno"),
                )
            )

            output_path = UPDATE_OUTPUT_ROOT / scenario["short"] / update_case["short"] / "emp"
            if output_path.exists():
                shutil.rmtree(output_path)

            label = f"{scenario['name']} - {update_case['name']}"
            start = time.perf_counter()
            write_by_format(updated, fmt, output_path)
            elapsed = time.perf_counter() - start
            print(f"{label}: {elapsed:,.2f}s")
            return elapsed


        update_results = []
        if RUN_UPDATE_BENCHMARKS:
            for scenario in scenarios:
                if not Path(scenario["base"]).exists():
                    print(f"Skipping missing folder: {scenario['base']}")
                    continue
                emp_files = next(item["files"] for item in stats if item["scenario"] == scenario["name"] and item["table"] == "emp")
                for update_case in update_cases:
                    elapsed = benchmark_emp_updates(scenario, update_case)
                    update_results.append({
                        "scenario": scenario["name"],
                        "short": scenario["short"],
                        "format": scenario["format"],
                        "layout": scenario["layout"],
                        "update_size": update_case["name"],
                        "update_records": update_case["records"],
                        "emp_files": emp_files,
                        "seconds": round(elapsed, 2),
                    })

            display(html_table(update_results, ["format", "layout", "update_size", "update_records", "emp_files", "seconds"]))
        else:
            display(HTML("<p style='font-family:Arial'>Update benchmarks are off. Set <code>RUN_UPDATE_BENCHMARKS = True</code> and rerun this cell to execute them.</p>"))
        """
    ),
    code(
        """
        if update_results:
            for fmt in ["csv", "json", "parquet"]:
                for layout in ["10 rows/file", "10,000 rows/file"]:
                    pair = rows_for_format_and_layout(update_results, fmt, layout)
                    if len(pair) == 2:
                        small, large = pair
                        display(comparison_card(
                            f"{fmt.upper()} {layout}: update runtime",
                            small,
                            large,
                            "seconds",
                            "Small update rewrites the same base dataset as the large update",
                            suffix=" sec",
                            color="#9333ea",
                        ))
        """
    ),
    md(
        """
        ## Benchmark 3: Join `emp`, `emp_projects`, and `projects`

        This is the big bridge-table join:

        ```python
        emp.join(emp_projects, "empno").join(projects, "projectno")
        ```

        It can process 100,000,000 bridge rows, so it is disabled by default. Turn it on only after reviewing the smaller benchmark, because this version is designed to show a heavier join workload.
        """
    ),
    code(
        """
        RUN_BIG_BRIDGE_JOIN = False

        def benchmark_big_bridge_join(scenario):
            spark.catalog.clearCache()
            gc.collect()
            emp = load_table(scenario, "emp").select("empno", "deptno")
            projects = load_table(scenario, "projects").select("projectno", "project_name")
            emp_projects = load_table(scenario, "emp_projects").select("empno", "projectno")
            joined = (
                emp_projects
                .join(F.broadcast(projects), "projectno", "inner")
                .join(emp, "empno", "inner")
            )
            return timed(scenario["name"], lambda: joined.count())


        bridge_results = []
        if RUN_BIG_BRIDGE_JOIN:
            for scenario in scenarios:
                if not scenario["has_emp_projects"]:
                    print(f"Skipping {scenario['name']} because emp_projects was intentionally not generated.")
                    continue
                elapsed, rows = benchmark_big_bridge_join(scenario)
                bridge_files = next(item["files"] for item in stats if item["scenario"] == scenario["name"] and item["table"] == "emp_projects")
                bridge_results.append({
                    "scenario": scenario["name"],
                    "format": scenario["format"],
                    "emp_project_files": bridge_files,
                    "join_rows": rows,
                    "seconds": round(elapsed, 2),
                })
            display(html_table(bridge_results, ["scenario", "format", "emp_project_files", "join_rows", "seconds"]))
            display(bar_chart(bridge_results, "scenario", "seconds", "Big Bridge Join Runtime", color="#16a34a", suffix=" sec"))
        else:
            display(HTML("<p style='font-family:Arial'>Big bridge join is off. Set <code>RUN_BIG_BRIDGE_JOIN = True</code> and rerun this cell to execute it.</p>"))
        """
    ),
    md(
        """
        ## Explain Plan: What Spark Is Doing

        Use this section after running the benchmarks. The plan shows scans, broadcast joins, exchanges, and adaptive execution.
        """
    ),
    code(
        """
        scenario = next(item for item in scenarios if item["short"] == "parquet_10000")
        emp = load_table(scenario, "emp")
        dept = load_table(scenario, "dept")

        emp.join(F.broadcast(dept), "deptno", "inner").explain("formatted")
        """
    ),
    md(
        """
        ## Review Questions

        Use these questions to check understanding after reviewing the tables, charts, and Spark execution plan:

        1. Which scenario has the most files for the same number of employee records?
        2. Does the slowest join always have the largest data size, or can file count dominate?
        3. Why does Parquet usually read less data than JSON or CSV?
        4. Why is `emp_projects` dangerous to generate with 10 rows per file?
        5. How would compaction help before running production joins?

        ## Answer Key

        1. The 10 rows-per-file scenarios have the most files. For `emp`, each tiny-file format has 100,000 files for the same 1,000,000 employee records, while the 10,000 rows-per-file version has only 100 files.
        2. The slowest join does not always have to be the largest dataset by size. File count can dominate because Spark must list files, read metadata, build scan tasks, and schedule work for every file. Many tiny files can make planning and scheduling expensive even when each file is small.
        3. Parquet usually reads less data because it is columnar, compressed, and stores schema and statistics. Spark can read only the columns needed for the query and can often skip unnecessary work. CSV and JSON are row-oriented text formats, so they usually require more parsing and more bytes scanned.
        4. `emp_projects` is dangerous with 10 rows per file because it has 100,000,000 records. At 10 rows per file, that would create 10,000,000 files, which can overwhelm directory listing, metadata handling, task scheduling, and local filesystem performance before the join itself becomes the main issue.
        5. Compaction combines many small files into fewer larger files. This reduces file listing, metadata reads, scan planning, and task scheduling overhead. The result is usually faster reads and joins, especially when the data is queried repeatedly.

        Practical takeaway: file format matters, but file layout matters too. A well-compressed columnar format can still perform badly when it is split into a very large number of tiny files.
        """
    ),
]


notebook = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {
            "name": "python",
            "pygments_lexer": "ipython3",
        },
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}


output_path = Path("02_join_performance_small_vs_large_files.ipynb")
output_path.write_text(json.dumps(notebook, indent=2), encoding="utf-8")
print(f"Wrote {output_path}")
