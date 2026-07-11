"""Shared runner used by each scenario's student-facing source generator."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def generate(scenario_dir: str, generator_args: list[str]) -> None:
    database_root = Path(__file__).resolve().parents[1]
    repo_root = database_root.parent
    output = repo_root / "data" / "database_scenarios" / scenario_dir
    command = [
        sys.executable,
        str(database_root / "scripts" / "generate_database_sources.py"),
        "--output-dir", str(output),
        *generator_args,
        "--overwrite",
    ]
    print("Step 1 - Generate deterministic source files")
    print(f"Local output: {output}")
    subprocess.run(command, check=True)
    print("Step 2 - Inspect the JSON manifest in the _manifests folder")
    print("Step 3 - Upload this output with scripts/upload_sources_to_minio.py")
