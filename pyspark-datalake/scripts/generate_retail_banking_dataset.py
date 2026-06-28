#!/usr/bin/env python3
"""Generate retail banking source batches for PySpark data lake demos."""

from __future__ import annotations

import argparse
import csv
import json
import random
import shutil
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable


FIRST_NAMES = ["Aarav", "Isha", "Rohan", "Meera", "Vikram", "Anika", "Jay", "Divya", "Kiran", "Neha"]
LAST_NAMES = ["Sharma", "Patel", "Rao", "Iyer", "Khan", "Mehta", "Nair", "Reddy", "Gupta", "Das"]
CITIES = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Chennai", "Pune", "Kolkata", "Ahmedabad"]
EMPLOYERS = ["Tinitiate Tech", "Northstar Retail", "Metro Health", "Bright Finance", "Urban Logistics"]
CHANNELS = ["upi", "debit_card", "net_banking", "atm", "branch"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate retail banking master and transaction batch files.")
    parser.add_argument("--output-dir", default="pyspark-datalake/data/source/retail_banking")
    parser.add_argument("--format", choices=["csv", "json"], default="csv")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--customers", type=int, default=1000)
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--start-date", default="2026-01-01")
    parser.add_argument("--small-files", type=int, default=4, help="Files per table per batch date.")
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def prepare_output_dir(path: Path, overwrite: bool) -> None:
    if path.exists():
        if not overwrite:
            raise FileExistsError(f"{path} exists. Use --overwrite to rebuild it.")
        shutil.rmtree(path)
    path.mkdir(parents=True)


def write_rows(base: Path, table: str, batch_date: date, fmt: str, small_files: int, rows: list[dict[str, object]]) -> None:
    table_dir = base / table / f"batch_date={batch_date.isoformat()}"
    table_dir.mkdir(parents=True, exist_ok=True)
    if not rows:
        return

    fieldnames = list(rows[0].keys())
    chunk_size = max(1, (len(rows) + small_files - 1) // small_files)
    for file_no, start in enumerate(range(0, len(rows), chunk_size), start=1):
        chunk = rows[start : start + chunk_size]
        if fmt == "csv":
            path = table_dir / f"{table}_part_{file_no:04d}.csv"
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(chunk)
        else:
            path = table_dir / f"{table}_part_{file_no:04d}.json"
            with path.open("w", encoding="utf-8") as handle:
                for row in chunk:
                    handle.write(json.dumps(row, separators=(",", ":")))
                    handle.write("\n")


def account_plans() -> list[dict[str, object]]:
    return [
        {"plan_id": 1, "plan_name": "salary_savings_standard", "account_type": "savings", "interest_rate": 3.25},
        {"plan_id": 2, "plan_name": "salary_savings_premium", "account_type": "savings", "interest_rate": 4.10},
        {"plan_id": 3, "plan_name": "everyday_checking", "account_type": "checking", "interest_rate": 0.00},
        {"plan_id": 4, "plan_name": "fixed_deposit_1_year", "account_type": "fixed_deposit", "interest_rate": 6.75},
        {"plan_id": 5, "plan_name": "fixed_deposit_3_year", "account_type": "fixed_deposit", "interest_rate": 7.15},
    ]


def loan_types() -> list[dict[str, object]]:
    return [
        {"loan_type_id": 1, "loan_type": "personal", "annual_rate": 12.5, "secured_flag": "N"},
        {"loan_type_id": 2, "loan_type": "auto", "annual_rate": 9.0, "secured_flag": "Y"},
        {"loan_type_id": 3, "loan_type": "home", "annual_rate": 8.2, "secured_flag": "Y"},
        {"loan_type_id": 4, "loan_type": "credit_card", "annual_rate": 36.0, "secured_flag": "N"},
    ]


def customers(count: int, rng: random.Random) -> list[dict[str, object]]:
    rows = []
    for customer_id in range(1, count + 1):
        rows.append(
            {
                "customer_id": customer_id,
                "first_name": rng.choice(FIRST_NAMES),
                "last_name": rng.choice(LAST_NAMES),
                "city": rng.choice(CITIES),
                "employer": rng.choice(EMPLOYERS),
                "kyc_status": rng.choice(["verified", "verified", "verified", "pending"]),
                "created_date": (date(2024, 1, 1) + timedelta(days=customer_id % 700)).isoformat(),
            }
        )
    return rows


def accounts(customer_rows: Iterable[dict[str, object]], rng: random.Random) -> list[dict[str, object]]:
    rows = []
    account_id = 100000
    for customer in customer_rows:
        for plan_id in [rng.choice([1, 2, 3])]:
            account_id += 1
            rows.append(
                {
                    "account_id": account_id,
                    "customer_id": customer["customer_id"],
                    "plan_id": plan_id,
                    "account_status": rng.choice(["active", "active", "active", "inactive"]),
                    "open_date": customer["created_date"],
                    "current_balance": round(rng.uniform(1000, 250000), 2),
                }
            )
    return rows


def daily_salary_deposits(account_rows: list[dict[str, object]], batch_date: date, rng: random.Random) -> list[dict[str, object]]:
    rows = []
    deposit_id_base = int(batch_date.strftime("%Y%m%d")) * 100000
    salary_accounts = [row for row in account_rows if row["plan_id"] in {1, 2}]
    for idx, account in enumerate(salary_accounts, start=1):
        if rng.random() < 0.22:
            rows.append(
                {
                    "deposit_id": deposit_id_base + idx,
                    "account_id": account["account_id"],
                    "customer_id": account["customer_id"],
                    "salary_month": batch_date.strftime("%Y-%m"),
                    "deposit_date": batch_date.isoformat(),
                    "amount": round(rng.uniform(25000, 250000), 2),
                    "employer_reference": f"PAY{batch_date.strftime('%Y%m%d')}{idx:06d}",
                }
            )
    return rows


def loans(customer_rows: list[dict[str, object]], rng: random.Random) -> list[dict[str, object]]:
    rows = []
    for loan_id, customer in enumerate(customer_rows[::5], start=500001):
        loan_type_id = rng.choice([1, 2, 3, 4])
        principal = rng.choice([75000, 250000, 850000, 3500000, 6500000])
        rows.append(
            {
                "loan_id": loan_id,
                "customer_id": customer["customer_id"],
                "loan_type_id": loan_type_id,
                "principal_amount": principal,
                "emi_amount": round(principal / rng.choice([24, 36, 60, 120]), 2),
                "start_date": (date(2025, 1, 1) + timedelta(days=loan_id % 300)).isoformat(),
                "loan_status": rng.choice(["active", "active", "active", "closed"]),
            }
        )
    return rows


def daily_payments(loan_rows: list[dict[str, object]], batch_date: date, rng: random.Random) -> list[dict[str, object]]:
    rows = []
    payment_id_base = int(batch_date.strftime("%Y%m%d")) * 100000
    for idx, loan in enumerate(loan_rows, start=1):
        if rng.random() < 0.18:
            rows.append(
                {
                    "payment_id": payment_id_base + idx,
                    "loan_id": loan["loan_id"],
                    "customer_id": loan["customer_id"],
                    "payment_date": batch_date.isoformat(),
                    "amount": loan["emi_amount"],
                    "payment_status": rng.choice(["paid", "paid", "paid", "failed"]),
                    "channel": rng.choice(CHANNELS),
                }
            )
    return rows


def daily_transactions(account_rows: list[dict[str, object]], batch_date: date, rng: random.Random) -> list[dict[str, object]]:
    rows = []
    transaction_id_base = int(batch_date.strftime("%Y%m%d")) * 1000000
    active_accounts = [row for row in account_rows if row["account_status"] == "active"]
    for idx, account in enumerate(active_accounts, start=1):
        for txn_no in range(rng.randint(0, 3)):
            rows.append(
                {
                    "transaction_id": transaction_id_base + (idx * 10) + txn_no,
                    "account_id": account["account_id"],
                    "customer_id": account["customer_id"],
                    "transaction_date": batch_date.isoformat(),
                    "transaction_type": rng.choice(["debit", "credit"]),
                    "amount": round(rng.uniform(100, 25000), 2),
                    "channel": rng.choice(CHANNELS),
                    "merchant_category": rng.choice(["salary", "grocery", "fuel", "emi", "shopping", "transfer"]),
                }
            )
    return rows


def daily_reminders(loan_rows: list[dict[str, object]], batch_date: date, rng: random.Random) -> list[dict[str, object]]:
    rows = []
    reminder_id_base = int(batch_date.strftime("%Y%m%d")) * 100000
    for idx, loan in enumerate(loan_rows, start=1):
        if rng.random() < 0.08:
            rows.append(
                {
                    "reminder_id": reminder_id_base + idx,
                    "loan_id": loan["loan_id"],
                    "customer_id": loan["customer_id"],
                    "reminder_date": batch_date.isoformat(),
                    "reminder_type": rng.choice(["sms", "email", "app_notification"]),
                    "reason": rng.choice(["emi_due", "payment_failed", "statement_ready"]),
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    output_dir = Path(args.output_dir)
    prepare_output_dir(output_dir, args.overwrite)

    start_date = date.fromisoformat(args.start_date)
    customer_rows = customers(args.customers, rng)
    account_rows = accounts(customer_rows, rng)
    loan_rows = loans(customer_rows, rng)
    plan_rows = account_plans()
    loan_type_rows = loan_types()

    for offset in range(args.days):
        batch_date = start_date + timedelta(days=offset)
        write_rows(output_dir, "customers", batch_date, args.format, args.small_files, customer_rows)
        write_rows(output_dir, "account_plans", batch_date, args.format, args.small_files, plan_rows)
        write_rows(output_dir, "accounts", batch_date, args.format, args.small_files, account_rows)
        write_rows(output_dir, "loan_types", batch_date, args.format, args.small_files, loan_type_rows)
        write_rows(output_dir, "loans", batch_date, args.format, args.small_files, loan_rows)
        write_rows(output_dir, "salary_deposits", batch_date, args.format, args.small_files, daily_salary_deposits(account_rows, batch_date, rng))
        write_rows(output_dir, "loan_payments", batch_date, args.format, args.small_files, daily_payments(loan_rows, batch_date, rng))
        write_rows(output_dir, "payment_reminders", batch_date, args.format, args.small_files, daily_reminders(loan_rows, batch_date, rng))
        write_rows(output_dir, "account_transactions", batch_date, args.format, args.small_files, daily_transactions(account_rows, batch_date, rng))

    print(f"Wrote retail banking {args.format} batches to {output_dir}")


if __name__ == "__main__":
    main()
