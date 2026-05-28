# PySpark Performance Tuning

## Purpose

This guide explains how Spark performance changes when the same data is stored in many small files versus fewer larger files. It focuses on join performance because joins are common in data engineering workloads and are sensitive to file layout, file format, partitioning, shuffling, and broadcast strategy.

The companion notebook is:

```text
02_join_performance_small_vs_large_files.ipynb
```

Use the notebook to run the benchmarks and view the charts. Use this markdown as the explanation and discussion guide.

## Teaching Runbook: Small Files to Larger Files

This section gives the complete classroom flow. The story is:

1. A source system sends many small files.
2. Spark loads the raw small files.
3. Loading is slower because Spark must list, plan, and scan many files.
4. We merge the small files into fewer larger files.
5. Spark processes the compacted files.
6. We compare runtimes before and after compaction.

Use Parquet first when teaching because the folder layout is simple and the performance lesson is clear. After students understand the idea, repeat the same pattern for CSV and JSON.

### Scripts Used

| Script | Purpose |
|---|---|
| `scripts/generate_training_csv.py` | Generates CSV training files. |
| `scripts/generate_training_json.py` | Generates JSON training files. |
| `scripts/generate_training_parquet.py` | Generates Parquet training files. |
| `scripts/generate_emp_update_files.py` | Generates small and large employee update files. |
| `scripts/compact_small_files.py` | Reads many small files and writes fewer larger files. |
| `scripts/create_join_performance_notebook.py` | Rebuilds the benchmark notebook if needed. |

### Step 1: Generate Small Source Files

This simulates a source system sending many tiny files. The example below creates Parquet files with 10 rows per file.

```powershell
python scripts/generate_training_parquet.py `
  --output-dir data/generated_parquet_10 `
  --overwrite `
  --rows-per-file 10 `
  --skip-emp-projects
```

Teaching point:

* The data volume can be normal, but the file count becomes very high.
* Spark must do work for each file, even when each file is tiny.

### Step 2: Generate Larger Files

This creates the same type of data with fewer, larger files.

```powershell
python scripts/generate_training_parquet.py `
  --output-dir data/generated_parquet_10000 `
  --overwrite `
  --rows-per-file 10000 `
  --skip-emp-projects
```

Teaching point:

* The logical data is similar.
* The file layout is different.
* Fewer files usually means less metadata and scheduling overhead.

### Step 3: Generate CSV and JSON Versions

Use these commands when you want to compare file formats also.

```powershell
python scripts/generate_training_json.py `
  --output-dir data/generated_json_10 `
  --overwrite `
  --rows-per-file 10 `
  --skip-emp-projects

python scripts/generate_training_json.py `
  --output-dir data/generated_json_10000 `
  --overwrite `
  --rows-per-file 10000 `
  --skip-emp-projects
```

```powershell
python scripts/generate_training_csv.py `
  --output-dir data/generated_csv_10 `
  --overwrite `
  --dept-rows-per-file 10 `
  --emp-rows-per-file 10 `
  --project-rows-per-file 10 `
  --salgrade-rows-per-file 10 `
  --skip-emp-projects

python scripts/generate_training_csv.py `
  --output-dir data/generated_csv_10000 `
  --overwrite `
  --dept-rows-per-file 10000 `
  --emp-rows-per-file 10000 `
  --project-rows-per-file 10000 `
  --salgrade-rows-per-file 10000 `
  --skip-emp-projects
```

Teaching point:

* Compare CSV with CSV, JSON with JSON, and Parquet with Parquet.
* Do not mix file-format performance with file-layout performance too early.

### Step 4: Count Files Before Loading

Use PowerShell to show students the physical file count.

```powershell
(Get-ChildItem data/generated_parquet_10/emp -Filter *.parquet).Count
(Get-ChildItem data/generated_parquet_10000/emp -Filter *.parquet).Count
```

Expected discussion:

* `data/generated_parquet_10/emp` should have many more files.
* `data/generated_parquet_10000/emp` should have far fewer files.
* Both folders represent the same table concept, but Spark sees very different input layouts.

### Step 5: Load Small Files in PySpark

Run this in a notebook cell or PySpark shell:

```python
import time
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("small-files-demo")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

start = time.perf_counter()
small_emp = spark.read.parquet("data/generated_parquet_10/emp")
small_count = small_emp.count()
small_seconds = time.perf_counter() - start

print(f"Small-file count: {small_count:,}")
print(f"Small-file load seconds: {small_seconds:,.2f}")
```

Teaching point:

* `count()` is the action that forces Spark to actually read the data.
* The DataFrame line is lazy; the timed work happens when `count()` runs.

### Step 6: Load Larger Files in PySpark

Run the same logic against the larger-file folder:

```python
start = time.perf_counter()
large_emp = spark.read.parquet("data/generated_parquet_10000/emp")
large_count = large_emp.count()
large_seconds = time.perf_counter() - start

print(f"Larger-file count: {large_count:,}")
print(f"Larger-file load seconds: {large_seconds:,.2f}")
```

Teaching point:

* The row counts should match.
* If the larger-file load is faster, the improvement is from layout, not different data.

### Step 7: Merge Small Files with the Script

Use the compaction script to merge small source files into fewer larger files.

```powershell
spark-submit scripts/compact_small_files.py `
  --format parquet `
  --input-path data/generated_parquet_10/emp `
  --output-path data/compacted/parquet_emp `
  --partitions 100
```

Teaching point:

* `repartition(100)` targets about 100 output files.
* The best partition count depends on data size and cluster size.
* The goal is not one giant file. The goal is fewer, healthier files.

### Step 8: Load the Compacted Files

Now read the compacted output:

```python
start = time.perf_counter()
compacted_emp = spark.read.parquet("data/compacted/parquet_emp")
compacted_count = compacted_emp.count()
compacted_seconds = time.perf_counter() - start

print(f"Compacted count: {compacted_count:,}")
print(f"Compacted load seconds: {compacted_seconds:,.2f}")
```

Teaching point:

* The compacted row count should match the small-file row count.
* The compacted load should usually be faster than repeatedly loading raw tiny files.

### Step 9: Compare Before and After

Use a small table to summarize the result:

```python
results = [
    ("small files", small_count, round(small_seconds, 2)),
    ("larger files", large_count, round(large_seconds, 2)),
    ("compacted files", compacted_count, round(compacted_seconds, 2)),
]

spark.createDataFrame(results, ["layout", "rows", "seconds"]).show(truncate=False)
```

Teaching point:

* File layout changes runtime even when row count is the same.
* Compaction is a common first step after receiving many small source files.

### Step 10: Run a Join After Compaction

Use the same join before and after compaction:

```python
from pyspark.sql import functions as F

dept = spark.read.parquet("data/generated_parquet_10000/dept")

start = time.perf_counter()
small_join_rows = small_emp.join(F.broadcast(dept), "deptno", "inner").count()
small_join_seconds = time.perf_counter() - start

start = time.perf_counter()
compacted_join_rows = compacted_emp.join(F.broadcast(dept), "deptno", "inner").count()
compacted_join_seconds = time.perf_counter() - start

print(f"Small-file join rows: {small_join_rows:,}, seconds: {small_join_seconds:,.2f}")
print(f"Compacted join rows: {compacted_join_rows:,}, seconds: {compacted_join_seconds:,.2f}")
```

Teaching point:

* Compaction helps downstream processing, not only raw loading.
* Broadcast joins are useful when the lookup table is small enough to fit in memory.

### Step 11: Run the Full Notebook

Open and run:

```text
02_join_performance_small_vs_large_files.ipynb
```

The notebook includes:

* folder and file statistics
* CSV, JSON, and Parquet layout comparisons
* `emp` join `dept`
* small update vs large update benchmarks
* optional large bridge-table join
* explain-plan discussion

### Step 12: Generate Update Files

Use this when teaching why small updates can still be expensive with plain files.

```powershell
python scripts/generate_emp_update_files.py `
  --output-dir data/update_records `
  --overwrite `
  --small-records 100 `
  --large-records 100000
```

Teaching point:

* Plain CSV, JSON, and Parquet files do not provide true row-level updates.
* A small update may still require reading and rewriting the full base dataset.

### Step 13: Optional Notebook Rebuild

If the generated notebook needs to be rebuilt from the script:

```powershell
python scripts/create_join_performance_notebook.py
```

Use this only when changing the notebook generator script.

## Core Idea

Spark can process large data in parallel, but every input file still has overhead.

For every file Spark may need to:

* list the file from storage
* read file metadata
* plan a scan task
* schedule a task
* open the file
* parse or decode the file content
* merge many small tasks into the final result

When data is split into too many tiny files, this overhead can become larger than the actual data-processing work.

## Dataset Layout

The benchmark compares each format against itself:

| Format | Tiny-file folder | Larger-file folder | Comparison |
|---|---:|---:|---|
| CSV | `data/generated_csv_10` | `data/generated_csv_10000` | CSV vs CSV |
| JSON | `data/generated_json_10` | `data/generated_json_10000` | JSON vs JSON |
| Parquet | `data/generated_parquet_10` | `data/generated_parquet_10000` | Parquet vs Parquet |

The tiny-file folders use 10 records per file. The larger-file folders use 10,000 records per file.

## Table Volumes

| Table | Records |
|---|---:|
| `dept` | 100 |
| `emp` | 1,000,000 |
| `projects` | 100 |
| `emp_projects` | 100,000,000 |
| `salgrade` | 1,000,000 |

`emp_projects` is skipped in the 10-record tiny-file folders. At 100,000,000 records and 10 records per file, it would create 10,000,000 files per format. That is intentionally avoided because it can overwhelm the local filesystem and make the demonstration impractical.

## File Count Example

For the `emp` table:

| Layout | Records | Rows Per File | File Count |
|---|---:|---:|---:|
| Tiny files | 1,000,000 | 10 | 100,000 |
| Larger files | 1,000,000 | 10,000 | 100 |

Both layouts contain the same number of employee records. The difference is the number of files Spark must discover, plan, and scan.

## Benchmark 1: `emp` Join `dept`

This benchmark uses a simple join:

```python
emp.join(F.broadcast(dept), "deptno", "inner").count()
```

Why this benchmark is useful:

* `emp` is large enough to show scan behavior.
* `dept` is small enough to broadcast.
* The join logic is simple.
* Runtime differences are easier to connect to file layout.

Expected result:

* The tiny-file layout usually takes longer than the larger-file layout for the same format.
* The difference is usually caused by file listing, metadata reads, scan planning, and task scheduling.

## Benchmark 2: `emp`, `emp_projects`, and `projects`

This benchmark joins a large bridge table:

```python
emp_projects.join(F.broadcast(projects), "projectno", "inner").join(emp, "empno", "inner").count()
```

This benchmark is disabled by default in the notebook because it can process 100,000,000 bridge records. Enable it only when you want to show a heavier join workload.

Expected result:

* The bridge table join is much heavier than the `emp` to `dept` join.
* The scan size, shuffle behavior, and join strategy become more important.
* File layout still matters, especially when Spark must plan thousands of input files.

## Update Performance: Small Updates vs Large Updates

Plain CSV, JSON, and Parquet files do not provide row-level update behavior by themselves. They are file-based storage formats. In most basic Spark workflows, an update is implemented by creating a new version of the data.

The common pattern is:

1. Read the base dataset.
2. Read the update records.
3. Join the base dataset to the update dataset by key.
4. Replace changed column values.
5. Write the updated result to a new output location.

Example:

```python
updated = (
    emp.alias("e")
    .join(F.broadcast(emp_updates).alias("u"), "empno", "left")
    .select(
        "empno",
        "ename",
        F.coalesce("new_job", "job").alias("job"),
        "mgr",
        "hiredate",
        F.coalesce("new_sal", "sal").alias("sal"),
        F.coalesce("new_commission", "commission").alias("commission"),
        "deptno",
    )
)
```

The benchmark uses two update sizes:

| Update Type | Records Updated |
|---|---:|
| Small update | 100 |
| Large update | 100,000 |

Both update types are applied to the same `emp` base table, which has 1,000,000 records.

### Why Small Updates Can Still Be Expensive

In a database with indexing and row-level storage, updating 100 records can be very cheap.

In plain files, updating 100 records can still require:

* scanning the full base dataset
* joining base records to update records
* rewriting the full updated output
* creating new output files
* committing the new output folder

That means a small update may not be much faster than a large update if the full base dataset must be rewritten in both cases.

### Small Updates

Small updates are useful for showing fixed overhead.

Expected behavior:

* The update file is small.
* The update side can usually be broadcast.
* Most of the runtime comes from reading and rewriting the base table.
* Tiny-file base layouts are usually slower because Spark must plan many input files.

### Large Updates

Large updates are useful for showing additional join and processing cost.

Expected behavior:

* The update file is larger.
* The update side may still be broadcast if it fits in memory.
* The join has more matching rows.
* The output still rewrites the full base table.
* Runtime may increase, but the increase may be smaller than expected because the full base rewrite is already the dominant cost.

### Update Benchmark Output

The notebook writes update benchmark results under:

```text
data/update_benchmark_output
```

Each run writes a new updated `emp` dataset for the selected format, layout, and update size.

### Update Performance Takeaway

For plain CSV, JSON, and Parquet:

* small updates are not true in-place updates
* large updates are also rewrite operations
* file count affects read planning
* output partitioning affects write performance
* compaction can improve future reads
* table formats such as Delta Lake, Apache Iceberg, and Apache Hudi are better choices when row-level updates, deletes, and merges are required regularly

## Why Compare CSV vs CSV, JSON vs JSON, and Parquet vs Parquet

Different formats have different performance characteristics. A CSV-vs-Parquet comparison mixes two separate topics:

* file layout
* file format

For a clean file-layout demonstration, compare each format against itself:

* CSV tiny files vs CSV larger files
* JSON tiny files vs JSON larger files
* Parquet tiny files vs Parquet larger files

This makes the conclusion clearer: the same format can perform very differently depending on file size and file count.

## File Format Behavior

### CSV

CSV is a row-oriented text format.

Important characteristics:

* easy to read and inspect
* no built-in schema
* usually requires parsing strings into data types
* often larger than Parquet
* repeated headers can add overhead when many files are created

CSV is useful for interoperability, but it is usually not the best format for large analytical workloads.

### JSON

JSON is also a text format, but it is more expressive than CSV.

Important characteristics:

* supports nested structures
* self-describing field names
* usually larger than CSV and Parquet
* requires more parsing work
* newline-delimited JSON works well with Spark

JSON is useful for semi-structured data, but it can be expensive for large joins because Spark must parse text records.

### Parquet

Parquet is a columnar binary format designed for analytics.

Important characteristics:

* stores schema
* supports compression
* stores data by column
* allows Spark to read only required columns
* supports statistics that can help skip unnecessary data

Parquet is usually preferred for analytical Spark workloads. However, Parquet can still perform poorly when the data is split into too many tiny files.

## Why Small Files Hurt Performance

Small files hurt performance because Spark has to do many small units of work instead of fewer efficient units of work.

Common symptoms:

* slow job startup
* many short tasks
* high scheduling overhead
* slow file listing
* slow metadata handling
* poor executor utilization
* excessive driver-side planning work

The job may look slow even when CPU and memory are not fully used. That often means the system is spending time on overhead rather than data processing.

## Why Larger Files Usually Help

Larger files reduce overhead.

Benefits:

* fewer files to list
* fewer file metadata reads
* fewer scan tasks
* less task scheduling overhead
* better throughput per task
* better compression efficiency in formats like Parquet

Larger files should not be too large. If files are extremely large, Spark may not have enough parallelism. The goal is balanced file sizes, not one huge file.

## Practical File Size Guidance

For many Spark workloads, a practical target is:

```text
128 MB to 1 GB per file
```

This is a general guideline, not a fixed rule. The best size depends on:

* cluster size
* storage system
* file format
* compression codec
* query pattern
* number of columns read
* amount of filtering
* join strategy

For local demonstrations, smaller files are acceptable because the goal is to show the performance pattern clearly.

## Join Performance Factors

Join performance depends on more than file count.

Important factors:

* input data size
* number of files
* file format
* compression
* number of partitions
* shuffle size
* join keys
* data skew
* broadcast eligibility
* available memory
* executor count and CPU cores

The benchmark isolates file layout as much as possible by comparing the same format against itself.

## Broadcast Joins

When one table is small, Spark can broadcast it to all executors.

Example:

```python
emp.join(F.broadcast(dept), "deptno", "inner")
```

Why this helps:

* avoids a large shuffle for the small table
* keeps the join simple
* reduces network movement

Broadcasting is appropriate when the smaller table fits comfortably in memory.

## Shuffle Joins

When both sides of a join are large, Spark usually needs to shuffle data.

During a shuffle:

* records are redistributed by join key
* data moves across the cluster
* intermediate shuffle files are written
* skewed keys can create slow tasks

The `emp_projects` join can demonstrate this more clearly because it involves a large bridge table.

## Data Skew

Data skew happens when some join keys have far more records than others.

Symptoms:

* most tasks finish quickly
* a few tasks run much longer
* one executor may do much more work
* the Spark UI shows uneven task durations

Common fixes:

* broadcast the smaller table when possible
* repartition by join key
* salt heavily skewed keys
* pre-aggregate before joining
* filter unnecessary records before joining

## Compaction

Compaction combines many small files into fewer larger files.

Example idea:

```python
df = spark.read.parquet("data/generated_parquet_10/emp")

(
    df.repartition(100)
    .write
    .mode("overwrite")
    .parquet("data/compacted/emp")
)
```

Why compaction helps:

* fewer files
* fewer tasks
* less metadata overhead
* better scan throughput
* better compression efficiency

Compaction is one of the most important maintenance activities in file-based data lakes.

## Repartition vs Coalesce

Use `repartition()` when you need to increase or rebalance partitions.

```python
df.repartition(100)
```

Use `coalesce()` when you want to reduce partitions without a full shuffle.

```python
df.coalesce(20)
```

General guidance:

* `repartition()` creates a shuffle but balances data better.
* `coalesce()` avoids a full shuffle but can create uneven partitions.
* For compaction, `repartition()` is often better when the existing files are highly uneven.

## Partitioning by Columns

Partitioning writes data into folder paths by column values.

Example:

```python
(
    emp
    .write
    .mode("overwrite")
    .partitionBy("deptno")
    .parquet("data/partitioned/emp")
)
```

Partitioning helps when queries filter by the partition column:

```python
spark.read.parquet("data/partitioned/emp").where("deptno = 10")
```

Partitioning can hurt when:

* there are too many distinct values
* each partition contains tiny files
* queries do not filter by the partition column

Do not partition by high-cardinality columns such as unique employee IDs.

## Caching

Caching can help when the same DataFrame is reused multiple times.

Example:

```python
emp = spark.read.parquet("data/generated_parquet_10000/emp")
emp.cache()
emp.count()
```

Important:

* The first action materializes the cache.
* Cache only data that is reused.
* Do not cache everything.
* Unpersist cached data when it is no longer needed.

```python
emp.unpersist()
```

## Reading Only Required Columns

With columnar formats like Parquet, selecting fewer columns can reduce I/O.

Example:

```python
emp = spark.read.parquet("data/generated_parquet_10000/emp").select("empno", "deptno")
```

This is less helpful for CSV and JSON because Spark usually has to parse full text records.

## Filtering Early

Apply filters before joins when possible.

Example:

```python
active_projects = projects.where("budget >= 500000")
result = emp_projects.join(F.broadcast(active_projects), "projectno")
```

Filtering early reduces:

* rows scanned
* rows shuffled
* rows joined
* memory pressure

## Spark Settings Used in the Notebook

The notebook uses a small number of settings:

```python
spark = (
    SparkSession.builder
    .appName("join-performance-small-vs-large-files")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.files.maxPartitionBytes", str(128 * 1024 * 1024))
    .getOrCreate()
)
```

### `spark.sql.adaptive.enabled`

Adaptive Query Execution allows Spark to adjust the query plan at runtime.

It can help with:

* changing join strategies
* coalescing shuffle partitions
* handling skewed joins

### `spark.sql.shuffle.partitions`

This controls the default number of shuffle partitions.

The default is often 200. For a local demonstration, 8 is easier to observe and avoids creating too many tiny shuffle tasks.

In production, tune this based on:

* data size
* cluster cores
* shuffle volume
* task duration

### `spark.sql.files.maxPartitionBytes`

This controls how many bytes Spark tries to pack into each file scan partition.

Larger values can reduce the number of scan tasks. Smaller values can increase parallelism.

## How to Read the Benchmark Results

Look at these values together:

| Metric | What It Means |
|---|---|
| File count | How many input files Spark has to plan and scan |
| Data size | How much data is stored on disk |
| Join runtime | End-to-end elapsed time for the join action |
| Join rows | Confirms that both layouts produce the same result |

The best comparison is within the same format:

* CSV tiny vs CSV larger
* JSON tiny vs JSON larger
* Parquet tiny vs Parquet larger

If the row count is the same but runtime is different, file layout is a major reason.

## Spark UI Observations

When running the notebook, open the Spark UI if available.

Look for:

* number of jobs
* number of stages
* number of tasks
* task duration distribution
* shuffle read and write size
* input size
* spill to disk

For tiny-file scans, expect many small tasks or longer planning time before tasks run.

## Review Questions

1. Which scenario has the most files for the same number of employee records?
2. Does the slowest join always have the largest data size, or can file count dominate?
3. Why does Parquet usually read less data than JSON or CSV?
4. Why is `emp_projects` dangerous to generate with 10 rows per file?
5. How would compaction help before running production joins?
6. Why should CSV be compared with CSV, JSON with JSON, and Parquet with Parquet?
7. When is a broadcast join useful?
8. What is the difference between `repartition()` and `coalesce()`?
9. Why can a small update still be expensive in plain CSV, JSON, or Parquet files?
10. Why are Delta Lake, Iceberg, or Hudi often better for frequent updates?

## Answer Key

1. The 10 rows-per-file scenarios have the most files. For `emp`, each tiny-file format has 100,000 files for the same 1,000,000 employee records. The 10,000 rows-per-file version has only 100 files.
2. The slowest join does not always have to be the largest dataset by size. File count can dominate because Spark must list files, read metadata, build scan tasks, and schedule work for every file.
3. Parquet usually reads less data because it is columnar, compressed, and stores schema and statistics. Spark can read only the columns needed for the query and may skip unnecessary work.
4. `emp_projects` is dangerous with 10 rows per file because it has 100,000,000 records. At 10 rows per file, it would create 10,000,000 files, which can overwhelm directory listing, metadata handling, task scheduling, and filesystem performance.
5. Compaction combines many small files into fewer larger files. This reduces file listing, metadata reads, scan planning, and task scheduling overhead.
6. Comparing the same format against itself isolates file layout. Comparing CSV to Parquet mixes file-layout effects with file-format effects.
7. A broadcast join is useful when one table is small enough to fit in memory on each executor. It avoids shuffling the small table across the cluster.
8. `repartition()` performs a shuffle and can rebalance data across partitions. `coalesce()` reduces partitions with less movement but may leave partitions uneven.
9. A small update can still be expensive because plain files usually require a full read, join, and rewrite of the updated dataset. The number of changed rows may be small, but the amount of data rewritten can still be large.
10. Delta Lake, Iceberg, and Hudi add table-level metadata and update/delete/merge capabilities. They can track changed files, manage transactions, and avoid treating every update as a simple manual rewrite workflow.

## Practical Takeaway

File format matters, but file layout matters too. A well-compressed columnar format can still perform badly when it is split into a very large number of tiny files.

For production-style Spark workloads:

* prefer Parquet for analytical data
* avoid excessive small files
* compact data regularly
* broadcast small dimension tables when appropriate
* filter early
* select only required columns
* watch shuffle size and skew
* use the Spark UI to confirm the actual bottleneck
