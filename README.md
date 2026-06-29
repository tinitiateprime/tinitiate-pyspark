<p align="center">
  <img src="tinitiate-pyspark.png" alt="TINITIATE PySpark Training" width="100%">
</p>

<h1 align="center">PySpark Training</h1>

<p align="center">
  A practical, notebook-based path from Spark fundamentals to data lakes, PostgreSQL ingestion, and performance tuning.
</p>

<p align="center">
  <a href="#quick-start">Quick start</a> •
  <a href="#curriculum">Curriculum</a> •
  <a href="#data-lake-operations">Data lake</a> •
  <a href="#pyspark-to-postgresql">PostgreSQL</a> •
  <a href="#performance-tuning">Performance</a>
</p>

---

## What is PySpark?

PySpark is the Python API for [Apache Spark](https://spark.apache.org/), an open-source engine for processing large datasets across one machine or a distributed cluster. It combines Python's approachable syntax with Spark's scalable execution engine, allowing the same code to grow from local exploration to production data pipelines.

PySpark provides DataFrames and Spark SQL for transforming structured data, along with tools for streaming, machine learning, and large-scale analytics. Its lazy execution model builds an optimized plan before running a job, helping teams process data efficiently without managing low-level distributed computing details.

## About this repository

This course teaches PySpark through focused Jupyter notebooks and hands-on examples. Start with DataFrame and Spark SQL fundamentals, progress through joins and window functions, then apply those skills to performance tuning and a MinIO-backed data lake.

### What you will learn

- Query, filter, group, and sort distributed data with PySpark
- Combine datasets with joins and set operations
- Clean and transform text with built-in Spark functions
- Build summaries with aggregate and analytical functions
- Diagnose small-file, shuffle, partitioning, and join-performance issues
- Load, compact, and validate retail-banking data in MinIO

## Quick start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Git
- A web browser for JupyterLab

### Run the training environment

```bash
git clone https://github.com/tinitiateprime/tinitiate-pyspark.git
cd tinitiate-pyspark/pyspark-basics/misc
docker compose -f pyspark-docker-setup.yml up -d
```

Open JupyterLab at [http://localhost:8888](http://localhost:8888). If it requests a token, retrieve the URL from the container logs:

```bash
docker logs jupyter
```

> [!NOTE]
> Before starting the containers, update the host path in the Jupyter volume mapping inside [`pyspark-docker-setup.yml`](pyspark-basics/misc/pyspark-docker-setup.yml) for your machine.

For more detail, see the [local PySpark setup guide](pyspark-basics/misc/pyspark-local-setup.md).

## Performance-first learning path

If performance engineering is your main goal, use this sequence:

1. Work through the [file I/O and data quality lab](performance_io.md) for large CSVs, tiny-file stress tests, ZIP extraction, error quarantine, and CSV/Parquet updates.
2. Run the [small-vs-large-file notebook](pyspark-notebooks/02_join_performance_small_vs_large_files.ipynb) and compare file listing, scan planning, task count, and runtime.
3. Use the [performance tuning guide](performance_tuning.md) for joins, shuffles, broadcast strategy, skew, partitioning, caching, and compaction.
4. Apply the patterns to the [MinIO data lake pipeline](pyspark-datalake/docs/batch-loads-minio.md) and validate both performance and row/file reconciliation.

## Curriculum

### 1. DQL clauses

Learn the building blocks of DataFrame queries and their Spark SQL equivalents.

| Lesson | Topics |
| --- | --- |
| [SELECT](pyspark-basics/dql_clauses/01_select.ipynb) | Choose, reorder, and alias output columns |
| [WHERE](pyspark-basics/dql_clauses/02_where.ipynb) | Filter rows with comparisons and Boolean expressions |
| [GROUP BY](pyspark-basics/dql_clauses/03_group_by.ipynb) | Group records and calculate summaries |
| [HAVING](pyspark-basics/dql_clauses/04_having.ipynb) | Filter aggregate results |
| [ORDER BY](pyspark-basics/dql_clauses/05_order_by.ipynb) | Sort by one or more columns |

[Read the DQL overview →](pyspark-basics/dql_clauses/README.md)

### 2. Joins

Combine related datasets and learn how each join type handles matched and unmatched rows.

| Lesson | Topics |
| --- | --- |
| [Inner join](pyspark-basics/joins/01_inner_join.ipynb) | Keep rows that match on both sides |
| [Left outer join](pyspark-basics/joins/02_left_outer_join.ipynb) | Keep all rows from the left dataset |
| [Right outer join](pyspark-basics/joins/03_right_outer_join.ipynb) | Keep all rows from the right dataset |
| [Full outer join](pyspark-basics/joins/04_full_outer_join.ipynb) | Keep matched and unmatched rows from both sides |
| [Multiple-table joins](pyspark-basics/joins/05_multiple_tables.ipynb) | Connect several related datasets |
| [Non-equi joins](pyspark-basics/joins/06_non_equi_join.ipynb) | Join with ranges and comparison conditions |

[Read the joins overview →](pyspark-basics/joins/README.md)

### 3. Set operations

Combine or compare compatible datasets while controlling duplicate rows.

| Lesson | Topics |
| --- | --- |
| [UNION](pyspark-basics/set_operations/01_union.ipynb) | Combine datasets and remove duplicates |
| [UNION ALL](pyspark-basics/set_operations/02_union_all.ipynb) | Combine datasets and retain duplicates |
| [INTERSECT](pyspark-basics/set_operations/03_intersect.ipynb) | Return rows found in both datasets |
| [EXCEPT](pyspark-basics/set_operations/04_except.ipynb) | Return rows found only in the first dataset |

[Read the set operations overview →](pyspark-basics/set_operations/README.md)

### 4. Operators

Build expressive filters with comparisons, ranges, patterns, lists, and existence checks.

| Lesson | Topics |
| --- | --- |
| [Equality and inequality](pyspark-basics/operators/01_equality_inequality.ipynb) | Equal and not-equal comparisons |
| [IN and NOT IN](pyspark-basics/operators/02_in_not_in.ipynb) | Include or exclude a list of values |
| [LIKE and NOT LIKE](pyspark-basics/operators/03_like_not_like.ipynb) | Match text patterns with wildcards |
| [BETWEEN](pyspark-basics/operators/04_between.ipynb) | Filter values within an inclusive range |
| [Comparisons](pyspark-basics/operators/05_comparisons.ipynb) | Greater-than and less-than comparisons |
| [EXISTS and NOT EXISTS](pyspark-basics/operators/06_exists_not_exists.ipynb) | Use subqueries, semi joins, and anti joins |

[Read the operators overview →](pyspark-basics/operators/README.md)

### 5. String functions

Clean, standardize, search, and reshape text data.

| Lesson | Topics |
| --- | --- |
| [Substring and concatenation](pyspark-basics/string_functions/01_substring_concatenation.ipynb) | Extract and combine text |
| [LOWER and UPPER](pyspark-basics/string_functions/02_lower_upper.ipynb) | Standardize letter case |
| [TRIM, LTRIM, and RTRIM](pyspark-basics/string_functions/03_trim_ltrim_rtrim.ipynb) | Remove unwanted spaces |
| [CHARINDEX](pyspark-basics/string_functions/04_charindex.ipynb) | Find one string inside another |
| [LEFT and RIGHT](pyspark-basics/string_functions/05_left_right.ipynb) | Extract text from either end |
| [REVERSE, REPLACE, and LENGTH](pyspark-basics/string_functions/06_reverse_replace_length.ipynb) | Transform and measure strings |

[Read the string functions overview →](pyspark-basics/string_functions/README.md)

### 6. Aggregate functions

Turn detailed records into useful totals, counts, averages, and grouped summaries.

| Lesson | Topics |
| --- | --- |
| [COUNT](pyspark-basics/aggregate_functions/01_count.ipynb) | Count rows and non-null values |
| [SUM and AVG](pyspark-basics/aggregate_functions/02_sum_avg.ipynb) | Calculate totals and averages |
| [MAX and MIN](pyspark-basics/aggregate_functions/03_max_min.ipynb) | Find highest and lowest values |
| [Aggregate by group](pyspark-basics/aggregate_functions/04_by_group.ipynb) | Summarize records by category |

[Read the aggregate functions overview →](pyspark-basics/aggregate_functions/README.md)

### 7. Analytical functions

Calculate rankings and row-to-row comparisons without collapsing the underlying dataset.

| Lesson | Topics |
| --- | --- |
| [ROW_NUMBER, RANK, and DENSE_RANK](pyspark-basics/analytical_functions/01_row_number_rank_dense_rank.ipynb) | Rank rows and handle ties |
| [NTILE](pyspark-basics/analytical_functions/02_ntile.ipynb) | Divide ordered rows into buckets |
| [LAG and LEAD](pyspark-basics/analytical_functions/03_lag_lead.ipynb) | Compare with previous and next rows |
| [FIRST_VALUE and LAST_VALUE](pyspark-basics/analytical_functions/04_first_last_value.ipynb) | Read boundary values in a window frame |

[Read the analytical functions overview →](pyspark-basics/analytical_functions/README.md)

## Data lake operations

Apply the course concepts to an end-to-end retail-banking pipeline backed by MinIO. The module covers source-data design, object-storage configuration, repeatable batch ingestion, small-file compaction, and validation.

| Module | What it covers |
| --- | --- |
| [1. Dataset design](pyspark-datalake/01_dataset_design/README.md) | Finance tables, columns, and source batch layout |
| [2. MinIO setup](pyspark-datalake/02_minio_setup/README.md) | Docker configuration, object storage, and Spark S3A settings |
| [3. Batch loads](pyspark-datalake/03_batch_loads/README.md) | Bulk, incremental, selected-table, and reprocessing patterns |
| [4. Small-file compaction](pyspark-datalake/04_compaction/README.md) | Consolidating source files into efficient Parquet output |
| [5. Validation](pyspark-datalake/05_validation/README.md) | Source checks, MinIO path checks, and PySpark queries |

### Data lake resources

- [End-to-end batch loading tutorial](pyspark-datalake/docs/batch-loads-minio.md)
- [Generate a retail-banking dataset](pyspark-datalake/scripts/generate_retail_banking_dataset.py)
- [Inspect generated source batches](pyspark-datalake/scripts/inspect_retail_banking_batches.py)
- [Load batches into MinIO](pyspark-datalake/scripts/batch_load_to_minio.py)

## PySpark to PostgreSQL DB

Learn to load CSV, JSON, and Parquet sources into PostgreSQL with explicit schemas, controlled JDBC concurrency, rejected-record handling, audit metrics, and restartable phases.

The database course provides 11 detailed scenarios. Every scenario includes its own generated dataset, manifest, runnable PowerShell script, PostgreSQL validation, performance questions, and cleanup guidance.

- Small and large files loaded into one target table
- Source folders routed into multiple related tables
- Customer, sales, product, location, employee, department, and project datasets
- An opt-in stress lab for up to one million source files
- Millions of updates and deletes through deduplicated staging and atomic SQL

- [Open the PostgreSQL tutorial](pyspark-database/README.md)
- [Browse all detailed scenarios](pyspark-database/scenarios/README.md)

## Repository structure

```text
tinitiate-pyspark/
├── pyspark-basics/       # Core lessons, notebooks, setup, and helper scripts
├── pyspark-database/     # Phase-based source-file loads into PostgreSQL
├── pyspark-datalake/     # MinIO-backed retail-banking pipeline
├── pyspark-notebooks/    # Combined and performance-focused notebooks
├── performance_io.md     # File loading, extraction, errors, and update lab
├── performance_tuning.md # Spark optimization guide
└── README.md
```

## Additional resources

- [Supporting files and setup notes](pyspark-basics/misc/README.md)
- [PySpark tutorial outline](pyspark_tutorials.md)
- [Training data helper scripts](pyspark-basics/scripts)
- [Combined DQL notebook](pyspark-notebooks/01_dql_select_where_group_having_order.ipynb)

## Performance tuning

Move beyond correct code and learn how ingestion design, file layout, data quality, partitioning, and Spark execution strategy affect runtime.

- Read large CSV extracts and up to 10,000 one-row CSV files per table
- Benchmark many Parquet files with 100,000 rows per file
- Extract ZIP archives safely and quarantine malformed CSV or corrupt Parquet files
- Compare CSV and Parquet update rewrites and understand when to use a table format
- Benchmark many small files against fewer large files across CSV, JSON, and Parquet
- Understand scan planning, task scheduling, shuffles, broadcast joins, and data skew
- Compare `repartition()` and `coalesce()` and tune common Spark SQL settings
- Compact small source files before downstream processing
- Use the Spark UI to inspect jobs, stages, tasks, spills, and shuffle activity

- [Run the file I/O and data quality lab](performance_io.md)
- [Read the performance tuning guide](performance_tuning.md)
- [Open the join-performance notebook](pyspark-notebooks/02_join_performance_small_vs_large_files.ipynb)

---

<p align="center">
  <strong> Jay Kumsi & Venkata Bhattaram </strong><br>
  © <a href="https://tinitiate.com">TINITIATE.COM</a>
</p>
