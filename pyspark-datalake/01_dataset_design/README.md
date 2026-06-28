# 01 Dataset Design: Finance Retail Banking

[Back to main README](../../README.md#data-lake-operations)

This folder explains the retail banking dataset used for the PySpark data lake lessons.

## Business Scenario

A retail bank receives daily batch files from operational systems. The files include customer master data, account master data, salary deposits, loan details, loan payments, reminders, and account transactions.

The teaching goal is to show how a realistic finance dataset moves from source batches into a MinIO-backed data lake.

## Data Areas

| Area | Tables | Type |
|---|---|---|
| Customer master | `customers` | Master data |
| Account master | `account_plans`, `accounts` | Master data |
| Salary banking | `salary_deposits` | Transactional data |
| Loans | `loan_types`, `loans` | Master data |
| Loan operations | `loan_payments`, `payment_reminders` | Transactional data |
| Account activity | `account_transactions` | Transactional data |

## Table Details

### `customers`

Customer profile table.

| Column | Meaning |
|---|---|
| `customer_id` | Unique customer identifier |
| `first_name` | Customer first name |
| `last_name` | Customer last name |
| `city` | Customer city |
| `employer` | Employer name for salary banking examples |
| `kyc_status` | KYC state such as `verified` or `pending` |
| `created_date` | Customer onboarding date |

### `account_plans`

Bank product plan table.

| Column | Meaning |
|---|---|
| `plan_id` | Unique account plan identifier |
| `plan_name` | Product name |
| `account_type` | Savings, checking, or fixed deposit |
| `interest_rate` | Annual interest rate |

### `accounts`

Customer account table.

| Column | Meaning |
|---|---|
| `account_id` | Unique account identifier |
| `customer_id` | Customer who owns the account |
| `plan_id` | Account plan |
| `account_status` | Active or inactive |
| `open_date` | Account opening date |
| `current_balance` | Current balance for demo reporting |

### `salary_deposits`

Salary deposit transaction table.

| Column | Meaning |
|---|---|
| `deposit_id` | Unique deposit identifier |
| `account_id` | Salary account |
| `customer_id` | Customer receiving salary |
| `salary_month` | Salary month |
| `deposit_date` | Date received |
| `amount` | Salary amount |
| `employer_reference` | Source payroll reference |

### `loan_types`

Loan product lookup table.

| Column | Meaning |
|---|---|
| `loan_type_id` | Unique loan type identifier |
| `loan_type` | Personal, auto, home, or credit card |
| `annual_rate` | Annual loan rate |
| `secured_flag` | Whether collateral exists |

### `loans`

Customer loan account table.

| Column | Meaning |
|---|---|
| `loan_id` | Unique loan identifier |
| `customer_id` | Borrower |
| `loan_type_id` | Loan product |
| `principal_amount` | Original principal |
| `emi_amount` | Monthly installment |
| `start_date` | Loan start date |
| `loan_status` | Active or closed |

### `loan_payments`

Loan EMI payment table.

| Column | Meaning |
|---|---|
| `payment_id` | Unique payment identifier |
| `loan_id` | Loan being paid |
| `customer_id` | Borrower |
| `payment_date` | Payment date |
| `amount` | Payment amount |
| `payment_status` | Paid or failed |
| `channel` | Payment channel |

### `payment_reminders`

Reminder events sent to customers.

| Column | Meaning |
|---|---|
| `reminder_id` | Unique reminder identifier |
| `loan_id` | Related loan |
| `customer_id` | Customer receiving reminder |
| `reminder_date` | Reminder date |
| `reminder_type` | SMS, email, or app notification |
| `reason` | EMI due, failed payment, or statement ready |

### `account_transactions`

Daily account activity table.

| Column | Meaning |
|---|---|
| `transaction_id` | Unique transaction identifier |
| `account_id` | Account used |
| `customer_id` | Customer |
| `transaction_date` | Transaction date |
| `transaction_type` | Debit or credit |
| `amount` | Transaction amount |
| `channel` | UPI, card, ATM, branch, or net banking |
| `merchant_category` | Demo merchant category |

## Source Folder Layout

Generated source files are stored by table and batch date:

```text
pyspark-datalake/data/source/retail_banking/
  customers/batch_date=2026-01-01/*.csv
  accounts/batch_date=2026-01-01/*.csv
  salary_deposits/batch_date=2026-01-01/*.csv
```

This layout lets students practice:

* daily batch loads
* partition discovery
* incremental processing by date
* small-file handling

## Generate the Dataset

From the repository root:

```powershell
python pyspark-datalake/scripts/generate_retail_banking_dataset.py `
  --output-dir pyspark-datalake/data/source/retail_banking `
  --format csv `
  --overwrite `
  --customers 1000 `
  --days 7 `
  --start-date 2026-01-01 `
  --small-files 4
```

Inspect the generated file counts:

```powershell
python pyspark-datalake/scripts/inspect_retail_banking_batches.py `
  --source-root pyspark-datalake/data/source/retail_banking `
  --format csv
```

Teaching point:

* Source systems often send multiple files for each table and batch date.
* The files may be CSV or JSON even when the data lake target is Parquet.
* The raw source layout is not always the best layout for downstream processing.

[Back to main README](../../README.md#data-lake-operations)
