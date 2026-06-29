# Scenario 04: Many Small CSV Files to the Employee Table

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Load many header-bearing CSV files into `training.emp` while validating dates, decimals, nullable values, and malformed records.

## Dataset

| Property | Default |
| --- | --- |
| Generated folder | `data/database_scenarios/04_many_small_csv_emp/04_csv_small_emp/emp` |
| Files | 20 |
| Rows per file | 10 |
| Expected rows | 200 |
| Target | `training.emp` |
| Primary key | `empno` |

Columns: `empno`, `ename`, `job`, `mgr`, `hiredate`, `sal`, `commission`, and `deptno`.

## Run the complete scenario

```powershell
./pyspark-database/scenarios/04_many_small_csv_emp/run.ps1
```

## Phase 1: Check prerequisites

The scenario verifies Python, Docker, and PostgreSQL before writing data.

## Phase 2: Generate CSV files

Every part contains the same header and deterministic employee rows. The schema is not inferred during loading; the reusable loader owns the expected contract.

## Phase 3: Inspect the manifest and samples

Verify delimiter, header, encoding, column count, and row totals. CSV can be structurally valid while containing invalid dates or decimal text, so physical inspection is not enough.

## Phase 4: Load employees

Spark reads the folder in permissive mode with a corrupt-record column, converts each field to its target type, and rejects records with missing required values or failed casts. Accepted rows are written in JDBC batches.

## Phase 5: Validate

Defaults should yield 200 employees and zero rejects. Review `data/database_rejects` if the rejected count is nonzero. Each rejected row includes source filename, error reason, raw payload, and corrupt-record content.

## Phase 6: Review and cleanup

Run with one row per file to make metadata overhead visible:

```powershell
./pyspark-database/scenarios/04_many_small_csv_emp/run.ps1 -FileCount 1000 -RowsPerFile 1
```

The generated folder stays available for inspection until manually removed.

## Best practices demonstrated

- Do not use CSV schema inference for recurring loads.
- Separate structural parse errors from type/business errors.
- Quarantine failed rows with lineage.
- Keep PostgreSQL connection count independent of CSV file count.
