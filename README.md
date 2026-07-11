# Tinitiate PySpark

PySpark learning repository for students who want to practice Spark basics, data loading, PostgreSQL integration, MinIO object storage, and performance tuning.

Use this README as the main index. Detailed steps are kept in separate markdown files.

## 1. Setup

Start here if you are new to this repository.

| Topic | Link |
|---|---|
| Download and extract the lab files | [Download and extract project](pyspark-database/DOWNLOAD_AND_EXTRACT_PROJECT.md) |
| Local PySpark setup | [PySpark local setup](pyspark-basics/misc/pyspark-local-setup.md) |
| Supporting setup notes | [Misc setup notes](pyspark-basics/misc/README.md) |

## 2. PySpark Basics

Start with these notebooks before moving to database or performance labs.

| Topic | Link |
|---|---|
| DQL clauses: SELECT, WHERE, GROUP BY, HAVING, ORDER BY | [DQL clauses](pyspark-basics/dql_clauses/README.md) |
| Joins: inner, left, right, full, multiple-table, non-equi | [Joins](pyspark-basics/joins/README.md) |
| Set operations: UNION, UNION ALL, INTERSECT, EXCEPT | [Set operations](pyspark-basics/set_operations/README.md) |
| Operators: IN, LIKE, BETWEEN, EXISTS, comparisons | [Operators](pyspark-basics/operators/README.md) |
| String functions | [String functions](pyspark-basics/string_functions/README.md) |
| Aggregate functions | [Aggregate functions](pyspark-basics/aggregate_functions/README.md) |
| Analytical/window functions | [Analytical functions](pyspark-basics/analytical_functions/README.md) |
| Combined DQL practice notebook | [DQL combined notebook](pyspark-notebooks/01_dql_select_where_group_having_order.ipynb) |

## 3. PySpark with PostgreSQL and MinIO

Use this section to learn how files are uploaded to MinIO and loaded into PostgreSQL using PySpark.

| Topic | Link |
|---|---|
| Complete student guide: Docker, PostgreSQL, MinIO, DBeaver, PySpark load | [MinIO to PostgreSQL student guide](pyspark-database/MINIO_TO_POSTGRES_SCENARIOS.md) |
| Optional: generate source files locally | [Generate files locally](pyspark-database/GENERATE_FILES_LOCALLY.md) |
| Dataset notes | [Database datasets](pyspark-database/DATASETS.md) |
| Scenario list | [Database scenarios](pyspark-database/scenarios/README.md) |

## 4. Data Lake

MinIO-backed data lake examples and batch loading workflows.

| Topic | Link |
|---|---|
| Dataset design | [Dataset design](pyspark-datalake/01_dataset_design/README.md) |
| MinIO setup | [MinIO setup](pyspark-datalake/02_minio_setup/README.md) |
| Batch loads | [Batch loads](pyspark-datalake/03_batch_loads/README.md) |
| Compaction | [Small-file compaction](pyspark-datalake/04_compaction/README.md) |
| Validation | [Validation](pyspark-datalake/05_validation/README.md) |
| End-to-end MinIO batch tutorial | [Batch loads with MinIO](pyspark-datalake/docs/batch-loads-minio.md) |

## 5. Performance Tuning

Use these after the basics.

| Topic | Link |
|---|---|
| File I/O and data quality lab | [Performance I/O lab](performance_io.md) |
| Spark performance tuning guide | [Performance tuning](performance_tuning.md) |
| Small vs large file performance notebook | [Join and file performance notebook](pyspark-notebooks/02_join_performance_small_vs_large_files.ipynb) |

## Recommended Learning Order

1. Complete the PySpark basics notebooks.
2. Run the PySpark with PostgreSQL and MinIO lab.
3. Practice the data lake examples.
4. Study performance tuning.

---

TINITIATE.COM
