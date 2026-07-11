#!/usr/bin/env python3
"""Scenario 11: generate base and CDC files in CSV, JSON, and Parquet."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("11_millions_updates_deletes", ["--scenario", "ultra", "cdc", "--ultra-format", "parquet", "--ultra-file-count", "2", "--ultra-rows-per-file", "100000", "--cdc-updates", "100000", "--cdc-deletes", "10000", "--cdc-rows-per-file", "10000", "--allow-ultra", "--all-formats"])
