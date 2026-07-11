#!/usr/bin/env python3
"""Scenario 02: generate small multi-table files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("02_many_small_json_multiple_tables", ["--scenario", "json-small-multi", "--small-file-count", "20", "--small-rows-per-file", "10", "--all-formats"])
