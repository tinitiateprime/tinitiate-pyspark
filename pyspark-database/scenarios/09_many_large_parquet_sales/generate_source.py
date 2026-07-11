#!/usr/bin/env python3
"""Scenario 09: generate large sales files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("09_many_large_parquet_sales", ["--scenario", "parquet-large-sales", "--large-file-count", "2", "--large-rows-per-file", "100000", "--all-formats"])
