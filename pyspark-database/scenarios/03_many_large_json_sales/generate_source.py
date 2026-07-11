#!/usr/bin/env python3
"""Scenario 03: generate large sales files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("03_many_large_json_sales", ["--scenario", "json-large-sales", "--large-file-count", "2", "--large-rows-per-file", "100000", "--all-formats"])
