# Scenario 11: Millions of File-Based Updates and Deletes

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Apply large change files to `training.sales_transaction` through validated Spark ingestion, deduplicated staging, and one atomic PostgreSQL set-based transaction.

## Dataset

| Property | Default lab |
| --- | ---: |
| Base Parquet files | 2 |
| Base rows/file | 100 |
| Base rows | 200 |
| Updates | 100 |
| Deletes | 25 |
| Change format | Parquet |
| Expected remaining rows | 175 |

Generated root: `data/database_scenarios/11_millions_updates_deletes`.

## Run the default scenario

```powershell
./pyspark-database/scenarios/11_millions_updates_deletes/run.ps1
```

Run five million updates and one million deletes only after the default succeeds:

```powershell
./pyspark-database/scenarios/11_millions_updates_deletes/run.ps1 `
  -BaseFileCount 60 -BaseRowsPerFile 100000 `
  -UpdateCount 5000000 -DeleteCount 1000000 `
  -ChangesPerFile 100000 -ChangeFormat parquet -ConfirmMillions
```

## Phase 1: Check CDC capacity

The script verifies that the base contains at least as many keys as updates plus deletes. One million or more changes requires explicit confirmation.

## Phase 2: Generate base and change files

The base contains deterministic transaction IDs. Change records contain `operation`, `transaction_id`, optional update values, and `change_ts`. CDC is generated in CSV, JSON, and Parquet so formats can be compared without changing business logic.

## Phase 3: Inspect manifests

Reconcile base and CDC separately. Confirm that update/delete keys fall inside the base key range. A valid change file can still reference a missing target key; that must be reported as unmatched.

## Phase 4: Load the base

The base Parquet data is loaded into `training.sales_transaction` with overwrite mode for a repeatable exercise. The count before CDC is displayed.

## Phase 5: Stage and apply changes

`apply_sales_transaction_cdc.py`:

1. Parses and validates change records.
2. Quarantines invalid operations or values.
3. Keeps the latest `change_ts` per transaction key.
4. Writes deduplicated changes to `training.sales_transaction_changes_staging`.
5. Calls `training.apply_sales_transaction_changes()`.
6. Updates and deletes in one PostgreSQL transaction.
7. Clears staging only after successful application.

No per-row database loop is used.

## Phase 6: Reconcile

The script checks remaining target rows, empty staging, and base-load audit data. CDC output reports accepted, rejected, staged, updated, deleted, and unmatched counts.

Default expectation: 200 base − 25 deletes = 175 remaining rows. Updates change values without changing the count.

## Phase 7: Review and cleanup

Test CSV, JSON, and Parquet CDC at the same change volume. Measure Spark validation/staging separately from PostgreSQL update/delete time. After large deletes, run `VACUUM (ANALYZE)` when operationally appropriate.

## Best practices demonstrated

- Treat source files as an immutable change log.
- Deduplicate by key and event time before staging.
- Apply changes with set-based transactional SQL.
- Reconcile missing keys and rejects explicitly.
- Design replay and batch IDs for idempotency.
