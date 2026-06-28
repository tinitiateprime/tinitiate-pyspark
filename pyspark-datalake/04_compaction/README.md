# 04 Small-File Compaction

[Back to main README](../../README.md#data-lake-operations)

This folder explains why small files are a data lake performance problem and how to compact them.

## Problem

Source systems may send many small files:

```text
salary_deposits/batch_date=2026-01-03/part_0001.csv
salary_deposits/batch_date=2026-01-03/part_0002.csv
salary_deposits/batch_date=2026-01-03/part_0003.csv
salary_deposits/batch_date=2026-01-03/part_0004.csv
```

Small files hurt Spark performance because Spark must:

* list many files
* read metadata for many files
* build many scan tasks
* schedule many small tasks
* open and close many files

The data volume may be small, but file overhead can still be high.

## Compaction Goal

Read raw source files once, then write fewer Parquet files:

```text
s3a://datalake/retail_banking/bronze_compacted/salary_deposits/batch_date=2026-01-03/
```

Downstream jobs should read compacted Parquet instead of repeatedly reading raw tiny CSV files.

## Compact One Batch Date

```powershell
spark-submit `
  --packages org.apache.hadoop:hadoop-aws:3.3.4 `
  pyspark-datalake/scripts/batch_load_to_minio.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --source-format csv `
  --target-format parquet `
  --load-type compact `
  --batch-date 2026-01-03 `
  --zone bronze `
  --bucket datalake `
  --output-partitions 2 `
  --mode overwrite
```

## How to Choose `--output-partitions`

For teaching:

* use `2` or `4` so students can see fewer output files
* use a small dataset so the run completes quickly

For production:

* choose partitions based on target file size
* avoid one giant file
* avoid thousands of tiny files
* tune by data volume, cluster size, and query pattern

## Before and After Discussion

Ask students:

| Question | Expected Learning |
|---|---|
| How many source files arrived? | Source layout may be noisy. |
| How many compacted files were written? | Compaction reduces file count. |
| What format is compacted output? | Parquet is better for analytics. |
| Which path should downstream jobs read? | Compacted zone, not raw source. |

## When to Compact

Compact when:

* a source sends many tiny files
* the same data is queried repeatedly
* downstream jobs spend too much time scanning files
* partition folders contain many small files

Avoid unnecessary compaction when:

* the data is read only once
* files are already a healthy size
* compaction costs more than the expected read improvement

[Back to main README](../../README.md#data-lake-operations)
