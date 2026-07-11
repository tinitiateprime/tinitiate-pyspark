#!/usr/bin/env python3
"""Scenario 06: generate large employee files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("06_many_large_csv_emp", ["--scenario", "csv-large-emp", "--large-file-count", "2", "--large-rows-per-file", "100000", "--all-formats"])
