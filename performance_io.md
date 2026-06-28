# PySpark File I/O and Data Quality Performance Lab

This lab focuses on the cost of discovering, opening, parsing, validating, and rewriting files. It complements the [main performance tuning guide](performance_tuning.md), which covers joins, shuffles, partitioning, caching, and compaction in greater depth.

## Coverage

| Scenario | Covered in this lab |
| --- | --- |
| Loading and extraction | Repeatable landing-to-bronze pattern with metrics and quarantine |
| Reading one large CSV | Explicit schemas, projection, filtering, partition sizing, and compression tradeoffs |
| Reading many small CSV files | One row per file, up to 10,000 files per table |
| Reading error files | Permissive parsing, corrupt-record capture, validation, and quarantine |
| ZIP extraction | Safe extraction before Spark reads the contained files |
| Many Parquet files | Configurable file count with 100,000 rows per file |
| Small lookup tables | A `products` example with 25 one-row files and broadcast guidance |
| Updates using Parquet | Join-and-rewrite behavior and production table-format guidance |
| Updates using CSV | Explicit parsing followed by join-and-rewrite |
| Bad data in CSV | Structural parse errors and logical validation errors |
| Bad data in Parquet | Corrupt binary files versus readable rows with invalid business values |

## 1. Generate the lab files

The fixture generator creates a large CSV, one-row CSV files, Parquet files, malformed data, a physically corrupt Parquet file, and a ZIP archive.

### Quick smoke dataset

```powershell
python pyspark-basics/scripts/generate_io_performance_fixtures.py `
  --output-dir data/io_performance `
  --overwrite
```

### Full 10,000-files-per-table experiment

This intentionally creates 30,025 tiny files: 10,000 files for each fact table and 25 files for the `products` lookup.

```powershell
python pyspark-basics/scripts/generate_io_performance_fixtures.py `
  --output-dir data/io_performance_10000 `
  --large-csv-rows 5000000 `
  --tiny-tables customers orders order_items `
  --tiny-files-per-table 10000 `
  --lookup-table products `
  --lookup-files 25 `
  --parquet-files 20 `
  --parquet-rows-per-file 100000 `
  --allow-high-file-count `
  --overwrite
```

> [!WARNING]
> Tens of thousands of files stress the local filesystem as well as Spark. Run the full experiment only in a disposable training directory and record generation time separately from Spark read time.

## 2. Use one explicit schema

Inferring a CSV schema requires an additional pass over the data and can produce inconsistent types between batches. Define the contract once and reuse it.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

io_schema = StructType([
    StructField("record_id", LongType(), True),
    StructField("product_id", LongType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_date", StringType(), True),
    StructField("_corrupt_record", StringType(), True),
])
```

Use a versioned schema in production. Treat unexpected schema changes as data-contract failures rather than silently inferring a new contract.

## 3. Reading a large CSV

```python
large_csv = (
    spark.read
    .schema(io_schema)
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .csv("data/io_performance/large_csv")
    .select("record_id", "product_id", "amount", "event_date", "_corrupt_record")
    .where(F.col("event_date") >= "2026-01-01")
)
```

Performance rules:

- Provide a schema instead of using `inferSchema`.
- Select only required columns and filter as early as possible.
- Prefer multiple reasonably sized files over one file that cannot provide enough parallel work.
- Plain CSV is splittable. A single gzip-compressed CSV is generally not splittable and may be processed by one task.
- Convert repeatedly queried raw CSV to Parquet once, then read Parquet downstream.
- Measure file bytes and task count; “large” should be defined by size, not only row count.

Useful scan settings to test—not blindly hard-code—include:

```python
spark.conf.set("spark.sql.files.maxPartitionBytes", 128 * 1024 * 1024)
spark.conf.set("spark.sql.files.openCostInBytes", 4 * 1024 * 1024)
```

## 4. Reading many small CSV files

```python
tiny_orders = (
    spark.read
    .schema(io_schema)
    .option("header", True)
    .csv("data/io_performance_10000/one_row_csv/orders/*.csv")
)
```

Spark must list, open, parse a header, schedule work, and close each file. With one row per file, metadata and task overhead can cost far more than processing the data.

Record these measurements:

```python
from time import perf_counter

started = perf_counter()
row_count = tiny_orders.count()
elapsed = perf_counter() - started

print({
    "rows": row_count,
    "input_files": len(tiny_orders.inputFiles()),
    "partitions": tiny_orders.rdd.getNumPartitions(),
    "seconds": round(elapsed, 2),
})
```

Compact after ingestion:

```python
(
    tiny_orders
    .drop("_corrupt_record")
    .repartition(4)
    .write
    .mode("overwrite")
    .parquet("data/io_performance_compacted/orders")
)
```

Target file size is more useful than target row count because row width and compression vary. Start around 128–512 MB per analytical Parquet file, measure, and adjust for the workload.

## 5. Small lookup files

Twenty-five one-row `products` files still have file-open overhead, but the resulting dataset is small enough to compact and broadcast.

```python
products = (
    spark.read
    .schema(io_schema)
    .option("header", True)
    .csv("data/io_performance_10000/one_row_csv/products/*.csv")
)

result = tiny_orders.join(F.broadcast(products), "product_id", "left")
```

Broadcasting avoids a shuffle during the join; it does not remove the cost of listing and reading the 25 source files. Compact stable lookups into one or a few Parquet files.

## 6. Reading many Parquet files with 100,000 rows each

```python
parquet_df = spark.read.parquet("data/io_performance_10000/many_parquets")
print("files:", len(parquet_df.inputFiles()))
print("partitions:", parquet_df.rdd.getNumPartitions())
print("rows:", parquet_df.count())
```

One hundred thousand rows per file is a reproducible lab setting, not a universal production target. Inspect actual compressed file size, row-group metadata, task duration, predicate pushdown, and selected columns. A narrow 100,000-row file may be tiny, while a wide one may be hundreds of megabytes.

```python
filtered = (
    parquet_df
    .select("record_id", "product_id", "amount")
    .where(F.col("product_id") == 1100)
)
filtered.explain("formatted")
```

## 7. Extracting ZIP files

Spark can transparently read compression codecs such as gzip in supported inputs, but a `.zip` archive is a container and should be extracted before passing its members to the CSV reader.

```python
from pathlib import Path
from zipfile import ZipFile

archive = Path("data/io_performance/zip/incoming_extract.zip")
extract_root = Path("data/io_performance/extracted").resolve()
extract_root.mkdir(parents=True, exist_ok=True)

with ZipFile(archive) as zip_file:
    for member in zip_file.infolist():
        destination = (extract_root / member.filename).resolve()
        if extract_root not in destination.parents and destination != extract_root:
            raise ValueError(f"Unsafe ZIP member: {member.filename}")
        zip_file.extract(member, extract_root)

extracted = (
    spark.read
    .schema(io_schema)
    .option("header", True)
    .csv(str(extract_root / "incoming" / "large_extract.csv"))
)
```

In a production load, extract to a unique staging directory, verify checksums and expected members, load the data, and archive or remove the staging directory only after the load commits successfully.

## 8. Bad data in CSV

There are two distinct failure classes:

1. **Structural errors**: broken quoting, missing or extra fields, and unreadable records.
2. **Logical errors**: parseable rows with invalid dates, negative amounts, missing keys, or failed business rules.

Capture malformed records instead of silently dropping them:

```python
raw_csv = (
    spark.read
    .schema(io_schema)
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("badRecordsPath", "data/io_performance/quarantine/csv_parser")
    .csv("data/io_performance/bad_csv")
)

structural_errors = raw_csv.where(F.col("_corrupt_record").isNotNull())

validated = (
    raw_csv
    .withColumn("parsed_date", F.to_date("event_date", "yyyy-MM-dd"))
    .withColumn(
        "error_reason",
        F.when(F.col("_corrupt_record").isNotNull(), "malformed_record")
        .when(F.col("record_id").isNull(), "missing_record_id")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("amount").isNull(), "invalid_amount")
        .when(F.col("amount") < 0, "negative_amount")
        .when(F.col("parsed_date").isNull(), "invalid_event_date"),
    )
)
logical_errors = validated.where(F.col("error_reason").isNotNull())
good_rows = validated.where(F.col("error_reason").isNull())
```

For production code, assign every input row a source filename and ingestion ID before validation. Write rejected rows with the rule name, original record, source file, and ingestion timestamp.

## 9. Bad data in Parquet

Parquet has two different failure modes:

- A **physically corrupt file** has invalid binary structure or unreadable metadata. Spark cannot recover individual rows from it.
- A **logically bad row** is valid Parquet but violates business rules. Spark can read it and route it to quarantine.

Fail fast is the safest default. To inventory a mixed directory while deliberately skipping corrupt files:

```python
spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")
readable_parquet = spark.read.parquet("data/io_performance/bad_parquet")
```

Do not enable `ignoreCorruptFiles` without reconciliation: skipped files can cause silent data loss. Compare discovered files with successfully read files and quarantine or re-request each corrupt object.

Validate readable rows normally:

```python
parquet_errors = readable_parquet.where(
    F.col("record_id").isNull()
    | F.col("product_id").isNull()
    | (F.col("amount") < 0)
    | F.to_date("event_date", "yyyy-MM-dd").isNull()
)
```

## 10. Updates using CSV and Parquet

CSV and Parquet are file formats, not transactional table formats. Neither supports an in-place row update. The basic pattern reads the base and updates, joins them by key, and writes a complete new version.

Generate both update formats:

```powershell
python pyspark-basics/scripts/generate_emp_update_files.py `
  --output-dir data/update_records `
  --small-records 100 `
  --large-records 100000 `
  --overwrite
```

Read CSV updates with an explicit schema; Parquet carries its physical schema:

```python
csv_updates = spark.read.schema(update_schema).option("header", True).csv(
    "data/update_records/small/emp_updates.csv"
)
parquet_updates = spark.read.parquet(
    "data/update_records/small/emp_updates.parquet"
)
```

Apply either update DataFrame with the same rewrite pattern:

```python
updated = (
    base.alias("b")
    .join(F.broadcast(parquet_updates).alias("u"), "empno", "left")
    .select(
        "empno",
        F.coalesce("u.new_job", "b.job").alias("job"),
        F.coalesce("u.new_sal", "b.sal").alias("sal"),
        F.coalesce("u.new_commission", "b.commission").alias("commission"),
        "b.deptno",
    )
)

updated.write.mode("overwrite").parquet("data/emp/version_2")
```

For frequent updates, use Delta Lake, Apache Iceberg, or Apache Hudi to gain transactional `MERGE`, snapshot, and file-rewrite management.

## 11. Loading and extraction checklist

A reliable load should make every stage measurable:

1. Discover files and create a manifest containing path, size, timestamp, and checksum where available.
2. Extract archives into an isolated staging location and reject unsafe or unexpected members.
3. Read with an explicit schema and preserve source filename and ingestion ID.
4. Separate structural parser failures from logical validation failures.
5. Write rejected records to quarantine with a reason and source metadata.
6. Convert accepted raw text data to compacted Parquet for downstream analytics.
7. Reconcile discovered, accepted, rejected, skipped, and written row/file counts.
8. Publish load duration, bytes, files, partitions, task time, shuffle, and output-file sizes.
9. Commit or promote the new dataset only after reconciliation succeeds.

## 12. Benchmark matrix

Run each case at least twice and treat the first run as warm-up when cache effects matter.

| Test | Change only this variable | Capture |
| --- | --- | --- |
| Large CSV | Explicit schema vs inference | Scan time, jobs, bytes, rows |
| Tiny CSV | 100 vs 1,000 vs 10,000 files | Listing time, tasks, scheduler delay |
| Compaction | Raw tiny CSV vs compacted Parquet | Runtime, file count, output bytes |
| Parquet | File count at 100,000 rows/file | Tasks, scan time, compressed size |
| Lookup | Regular join vs broadcast | Shuffle bytes and runtime |
| Bad CSV | Permissive load with validation | Accepted/rejected counts and cost |
| Bad Parquet | Fail fast vs audited skip | Failed/skipped files and reconciliation |
| Updates | 100 vs 100,000 changed rows | Read, join, rewrite, and output time |

The Spark UI is the source of truth for task count, scheduling delay, input bytes, shuffle, spill, and skew. Wall-clock time alone is not enough to explain a result.

[Back to the main README](README.md#performance-tuning)
