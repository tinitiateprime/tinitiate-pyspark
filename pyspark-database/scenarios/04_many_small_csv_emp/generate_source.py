#!/usr/bin/env python3
"""Scenario 04: generate small employee files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("04_many_small_csv_emp", ["--scenario", "csv-small-emp", "--small-file-count", "20", "--small-rows-per-file", "10", "--all-formats"])
