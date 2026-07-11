#!/usr/bin/env python3
"""Scenario 05: generate small multi-table files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("05_many_small_csv_multiple_tables", ["--scenario", "csv-small-multi", "--small-file-count", "20", "--small-rows-per-file", "10", "--all-formats"])
