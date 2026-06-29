# Scenario 05: Many Small CSV Files to Multiple Employee Tables

[Back to scenario index](../README.md) | [Main database tutorial](../../README.md)

## Goal

Load department, project, employee, and employee-project CSV folders into four PostgreSQL tables with explicit routing and dependency order.

## Dataset

| Table | Default files | Rows/file | Expected rows | Key |
| --- | ---: | ---: | ---: | --- |
| `dept` | 20 | 10 | 200 | `deptno` |
| `projects` | 20 | 10 | 200 | `project_id` |
| `emp` | 20 | 10 | 200 | `empno` |
| `emp_projects` | 20 | 10 | 200 | `emp_project_id` |

Generated root: `data/database_scenarios/05_many_small_csv_multiple_tables/05_csv_small_multi`.

## Run the complete scenario

```powershell
./pyspark-database/scenarios/05_many_small_csv_multiple_tables/run.ps1
```

## Phase 1: Check prerequisites

The script fails before generation if Docker or the isolated PostgreSQL service is unavailable.

## Phase 2: Generate four CSV datasets

Each target receives a dedicated folder and schema. The generator keeps IDs deterministic so joins can be tested after the load.

## Phase 3: Inspect and reconcile

Confirm four manifest entries and expected files per table. Inspect headers independently; a valid `emp` header says nothing about the `emp_projects` contract.

## Phase 4: Load in dependency order

The script loads `dept`, `projects`, `emp`, then `emp_projects`. Smaller reference tables use two JDBC writers; larger relationship tables use four.

This pattern scales to production orchestration: one controlled task per table, explicit dependencies, independent audits, and retry boundaries.

## Phase 5: Validate tables and relationships

Defaults produce 200 rows per table. The script displays table counts and all four audit records. Add relationship checks:

```sql
SELECT COUNT(*) AS assignments_without_employee
FROM training.emp_projects ep
LEFT JOIN training.emp e ON e.empno = ep.empno
WHERE e.empno IS NULL;
```

## Phase 6: Review and cleanup

Decide which tables can load concurrently without violating dependencies or exhausting PostgreSQL. Keep independent retry state so one failed table does not force every successful table to reload.

## Best practices demonstrated

- Map each source folder to an approved target and schema.
- Model load order explicitly.
- Audit each table separately.
- Validate cross-table relationships after row reconciliation.
