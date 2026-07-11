#!/usr/bin/env python3
"""Scenario 10: generate safe 10,000-file stress datasets in all formats."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts"))
from scenario_source import generate
generate("10_ultra_one_million_files", ["--scenario", "ultra", "--ultra-format", "json", "--ultra-file-count", "10000", "--ultra-rows-per-file", "1", "--allow-ultra", "--all-formats"])
