#!/usr/bin/env python3
"""Scenario 07: generate small transaction files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("07_many_small_parquet_transaction", ["--scenario", "parquet-small-transaction", "--small-file-count", "20", "--small-rows-per-file", "10", "--all-formats"])
