# PySpark Performance Tuning

## Purpose

This guide explains how Spark performance changes when the same data is stored in many small files versus fewer larger files. It focuses on join performance because joins are common in data engineering workloads and are sensitive to file layout, file format, partitioning, shuffling, and broadcast strategy.

The companion notebook is:

```text
02_join_performance_small_vs_large_files.ipynb
```

Use the notebook to run the benchmarks and view the charts. Use this markdown as the explanation and discussion guide.

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
