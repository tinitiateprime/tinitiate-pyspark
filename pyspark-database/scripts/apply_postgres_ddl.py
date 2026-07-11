#!/usr/bin/env python3
"""Apply the PostgreSQL training DDL inside the Docker PostgreSQL container."""

from __future__ import annotations

import argparse
import subprocess
from pathlib import Path


LAB_POSTGRES_CONTAINER_NAMES = ("postgres", "ti-batch-postgres")
POSTGRES_CONTAINER_HINTS = ("postgres", "pgvector")
EXCLUDED_CONTAINER_HINTS = ("openmetadata", "metadata")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sql-file", default="pyspark-database/sql/01_schema.sql")
    parser.add_argument(
        "--container",
        default="postgres",
        help=(
            "Lab PostgreSQL Docker container name. The script tries this first, "
            "then tries known lab names such as ti-batch-postgres. "
            "OpenMetadata containers are ignored."
        ),
    )
    parser.add_argument("--database", default="tinitiateai")
    parser.add_argument("--db-user", default="ti_dbuser")
    parser.add_argument("--db-password", default="tiuser!23456")
    return parser.parse_args()


def run_capture(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, capture_output=True, text=True, check=False)


def container_exists(container: str) -> bool:
    result = run_capture(["docker", "inspect", container])
    return result.returncode == 0


def detect_postgres_container(preferred_container: str) -> str:
    if container_exists(preferred_container):
        return preferred_container

    for container_name in LAB_POSTGRES_CONTAINER_NAMES:
        if container_name != preferred_container and container_exists(container_name):
            print(f"Container '{preferred_container}' was not found.")
            print(f"Using lab PostgreSQL container: {container_name}")
            return container_name

    result = run_capture(["docker", "ps", "--format", "{{.Names}}\t{{.Image}}"])
    if result.returncode != 0:
        raise RuntimeError(
            "Could not talk to Docker. Make sure Docker Desktop is running, then try again.\n"
            f"Docker error:\n{result.stderr.strip()}"
        )

    candidates: list[str] = []
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        name, _, image = line.partition("\t")
        combined = f"{name} {image}".lower()
        if any(hint in combined for hint in EXCLUDED_CONTAINER_HINTS):
            continue
        if any(hint in combined for hint in POSTGRES_CONTAINER_HINTS):
            candidates.append(name)

    if len(candidates) == 1:
        print(f"Container '{preferred_container}' was not found.")
        print(f"Auto-detected PostgreSQL container: {candidates[0]}")
        return candidates[0]

    if len(candidates) > 1:
        candidate_list = "\n".join(f"  - {candidate}" for candidate in candidates)
        raise RuntimeError(
            "More than one possible PostgreSQL container was found.\n"
            "Run the script again with the correct container name, for example:\n\n"
            "  C:\\Python311\\python.exe pyspark-database/scripts/apply_postgres_ddl.py "
            "--container ti-batch-postgres\n\n"
            "Do not choose an OpenMetadata PostgreSQL container for this lab.\n\n"
            f"Possible containers:\n{candidate_list}"
        )

    raise RuntimeError(
        f"Docker container '{preferred_container}' was not found, and no running "
        "PostgreSQL container could be auto-detected.\n\n"
        "Start the Docker containers first, then run this script again.\n"
        "If your PostgreSQL container has a custom name, pass it like this:\n\n"
        "  C:\\Python311\\python.exe pyspark-database/scripts/apply_postgres_ddl.py "
        "--container ti-batch-postgres\n\n"
        "Do not use OpenMetadata containers for this lab. This lab only uses "
        "the PostgreSQL database tinitiateai and the MinIO bucket datalake."
    )


def run_psql(args: argparse.Namespace, sql: str) -> None:
    command = [
        "docker",
        "exec",
        "-i",
        "-e",
        f"PGPASSWORD={args.db_password}",
        args.container,
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        args.db_user,
        "-d",
        args.database,
    ]
    subprocess.run(command, input=sql, text=True, check=True)


def verify_tables(args: argparse.Namespace) -> None:
    command = [
        "docker",
        "exec",
        "-e",
        f"PGPASSWORD={args.db_password}",
        args.container,
        "psql",
        "-U",
        args.db_user,
        "-d",
        args.database,
        "-c",
        r"\dt training.*",
    ]
    subprocess.run(command, check=True)


def main() -> None:
    args = parse_args()
    sql_file = Path(args.sql_file)
    if not sql_file.is_file():
        raise FileNotFoundError(f"SQL file not found: {sql_file}")

    args.container = detect_postgres_container(args.container)

    print(f"Applying DDL from {sql_file}")
    print(f"Target: container={args.container}, database={args.database}, user={args.db_user}")
    run_psql(args, sql_file.read_text(encoding="utf-8"))
    print("DDL applied successfully. Verifying training tables...")
    verify_tables(args)


if __name__ == "__main__":
    main()
