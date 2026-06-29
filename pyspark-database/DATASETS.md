# PostgreSQL Tutorial Data Dictionary

[Back to database tutorial](README.md) | [Scenario index](scenarios/README.md)

All datasets are deterministic: the same generator arguments produce the same keys and values. Generated files live under `data/database_scenarios` and are ignored by Git.

## Retail datasets

### `location`

| Column | Type | Meaning |
| --- | --- | --- |
| `location_id` | BIGINT | Primary business key |
| `location_name` | STRING | Generated location name |
| `region` | STRING | North, south, east, or west region |

### `product`

| Column | Type | Meaning |
| --- | --- | --- |
| `product_id` | BIGINT | Product key |
| `product_name` | STRING | Generated product name |
| `category` | STRING | Product category |
| `unit_price` | DECIMAL(14,2) | Current unit price |
| `active` | BOOLEAN | Active-product indicator |

### `customer`

| Column | Type | Meaning |
| --- | --- | --- |
| `customer_id` | BIGINT | Customer key |
| `customer_name` | STRING | Generated customer name |
| `email` | STRING | Generated email address |
| `location_id` | BIGINT | Logical reference to `location` |
| `created_at` | TIMESTAMP | Customer creation timestamp |

### `sales`

| Column | Type | Meaning |
| --- | --- | --- |
| `sale_id` | BIGINT | Sale key |
| `customer_id` | BIGINT | Logical customer reference |
| `product_id` | BIGINT | Logical product reference |
| `location_id` | BIGINT | Logical location reference |
| `quantity` | INTEGER | Units sold |
| `unit_price` | DECIMAL(14,2) | Sale-time unit price |
| `sale_ts` | TIMESTAMP | Sale timestamp |

Logical load order: `location` and `product`, then `customer`, then `sales`.

## Employee and project datasets

### `dept`

| Column | Type | Meaning |
| --- | --- | --- |
| `deptno` | BIGINT | Department key |
| `dname` | STRING | Department name |
| `loc` | STRING | Department location label |

### `emp`

| Column | Type | Meaning |
| --- | --- | --- |
| `empno` | BIGINT | Employee key |
| `ename` | STRING | Employee name |
| `job` | STRING | Job title |
| `mgr` | BIGINT | Nullable manager employee number |
| `hiredate` | DATE | Hire date |
| `sal` | DECIMAL(14,2) | Salary |
| `commission` | DECIMAL(14,2) | Nullable commission |
| `deptno` | BIGINT | Logical department reference |

### `projects`

| Column | Type | Meaning |
| --- | --- | --- |
| `project_id` | BIGINT | Project key |
| `project_name` | STRING | Project name |
| `budget` | DECIMAL(16,2) | Project budget |
| `location_id` | BIGINT | Logical location reference |

### `emp_projects`

| Column | Type | Meaning |
| --- | --- | --- |
| `emp_project_id` | BIGINT | Assignment key |
| `empno` | BIGINT | Logical employee reference |
| `project_id` | BIGINT | Logical project reference |
| `start_date` | DATE | Assignment start |
| `end_date` | DATE | Nullable assignment end |

Logical load order: `dept` and `projects`, then `emp`, then `emp_projects`.

## Transaction and CDC datasets

### `sales_transaction`

| Column | Type | Meaning |
| --- | --- | --- |
| `transaction_id` | BIGINT | Transaction key |
| `customer_id` | BIGINT | Logical customer reference |
| `product_id` | BIGINT | Logical product reference |
| `location_id` | BIGINT | Logical location reference |
| `amount` | DECIMAL(16,2) | Transaction amount |
| `transaction_ts` | TIMESTAMP | Transaction timestamp |

### `sales_transaction_changes`

| Column | Type | Meaning |
| --- | --- | --- |
| `operation` | CHAR(1) | `U` for update or `D` for delete |
| `transaction_id` | BIGINT | Target transaction key |
| `amount` | DECIMAL(16,2) | New amount for updates; null for deletes |
| `transaction_ts` | TIMESTAMP | New timestamp for updates; null for deletes |
| `change_ts` | TIMESTAMP | Event ordering and deduplication timestamp |

The CDC loader retains the latest `change_ts` per `transaction_id`, writes changes to `sales_transaction_changes_staging`, and invokes one atomic PostgreSQL function.

## Dataset profiles

| Profile | Purpose | Default layout |
| --- | --- | --- |
| Small files | File-open and scheduler overhead | 20 files × 10 rows/table |
| Large files | Parse/scan and JDBC throughput | 2 files × 100,000 rows/table |
| Ultra files | File discovery and planning stress | 10,000 files × 1 row by default; up to 1,000,000 opt-in |
| CDC | Update/delete staging | 100 updates + 25 deletes by default |

## Generated layout

```text
data/database_scenarios/<scenario-run>/
├── _manifests/
│   └── <scenario>.json
└── <generator-scenario>/
    ├── <table>/
    │   ├── part_0000001.<csv|json|parquet>
    │   └── ...
    └── ...
```

Manifests are intentionally stored outside table folders so Spark never mistakes them for source records.

## Reconciliation contract

For every table, verify:

```text
expected manifest rows
  = accepted rows + rejected rows

target rows after an overwrite load
  = accepted rows
```

For append, replay, update, or delete workflows, also account for pre-existing keys, duplicates, unmatched keys, and transactional outcomes.
