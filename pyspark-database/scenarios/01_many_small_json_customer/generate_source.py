#!/usr/bin/env python3
"""Scenario 01: generate small customer files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("01_many_small_json_customer", ["--scenario", "json-small-customer", "--small-file-count", "20", "--small-rows-per-file", "10", "--all-formats"])
