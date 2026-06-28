#!/usr/bin/env python3
"""Inspect retail banking source batches and show file counts by table/date."""

from __future__ import annotations

import argparse
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show retail banking source batch file counts.")
    parser.add_argument("--source-root", default="pyspark-datalake/data/source/retail_banking")
    parser.add_argument("--format", choices=["csv", "json", "parquet"], default="csv")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source_root = Path(args.source_root)
    if not source_root.exists():
        raise FileNotFoundError(f"{source_root} does not exist. Generate the dataset first.")

    rows = []
    for table_dir in sorted(path for path in source_root.iterdir() if path.is_dir()):
        for batch_dir in sorted(path for path in table_dir.iterdir() if path.is_dir()):
            files = list(batch_dir.glob(f"*.{args.format}"))
            rows.append((table_dir.name, batch_dir.name.replace("batch_date=", ""), len(files)))

    print(f"Source root: {source_root}")
    print(f"Format: {args.format}")
    print()
    print(f"{'table':<24} {'batch_date':<12} {'files':>8}")
    print("-" * 48)
    for table, batch_date, file_count in rows:
        print(f"{table:<24} {batch_date:<12} {file_count:>8}")


if __name__ == "__main__":
    main()
