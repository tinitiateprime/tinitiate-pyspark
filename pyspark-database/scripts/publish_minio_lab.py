#!/usr/bin/env python3
"""Generate lab sources and publish scenario folders plus DDL to MinIO."""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

try:
    from minio import Minio
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing Python package: minio\n\n"
        "Install it with:\n\n"
        "  C:\\Python311\\python.exe -m pip install --user minio\n"
    ) from exc


SCENARIOS = [
    "01_many_small_json_customer",
    "02_many_small_json_multiple_tables",
    "03_many_large_json_sales",
    "04_many_small_csv_emp",
    "05_many_small_csv_multiple_tables",
    "06_many_large_csv_emp",
    "07_many_small_parquet_transaction",
    "08_many_small_parquet_multiple_tables",
    "09_many_large_parquet_sales",
    "10_ultra_one_million_files",
]

OPTIONAL_SCENARIOS = [
    "11_millions_updates_deletes",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "datalake"))
    parser.add_argument("--prefix", default="", help="Optional MinIO prefix. Leave empty for bucket root.")
    parser.add_argument("--endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    parser.add_argument(
        "--include-heavy",
        action="store_true",
        help="Also publish Scenario 11. This creates larger base and CDC datasets.",
    )
    parser.add_argument(
        "--skip-generate",
        action="store_true",
        help="Upload existing data/database_scenarios folders without regenerating them.",
    )
    return parser.parse_args()


def minio_client(endpoint_url: str, access_key: str, secret_key: str) -> Minio:
    endpoint = urlparse(endpoint_url)
    if not endpoint.hostname:
        raise ValueError("--endpoint must include http:// or https://")
    host = endpoint.hostname if endpoint.port is None else f"{endpoint.hostname}:{endpoint.port}"
    return Minio(host, access_key=access_key, secret_key=secret_key, secure=endpoint.scheme == "https")


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def object_prefix(prefix: str, folder: str) -> str:
    clean_prefix = prefix.strip("/")
    clean_folder = folder.strip("/")
    return f"{clean_prefix}/{clean_folder}" if clean_prefix else clean_folder


def upload_directory(client: Minio, bucket: str, local_root: Path, remote_folder: str, prefix: str) -> None:
    if not local_root.is_dir():
        raise FileNotFoundError(f"Missing local folder: {local_root}")

    files = sorted(path for path in local_root.rglob("*") if path.is_file())
    if not files:
        raise ValueError(f"No files found below {local_root}")

    remote_root = object_prefix(prefix, remote_folder)
    for number, path in enumerate(files, 1):
        object_name = f"{remote_root}/{path.relative_to(local_root).as_posix()}"
        client.fput_object(bucket, object_name, str(path))
        if number == 1 or number % 1000 == 0 or number == len(files):
            print(f"Uploaded {number:,}/{len(files):,}: s3a://{bucket}/{object_name}")


def upload_file(client: Minio, bucket: str, local_file: Path, remote_name: str, prefix: str) -> None:
    if not local_file.is_file():
        raise FileNotFoundError(f"Missing local file: {local_file}")
    object_name = object_prefix(prefix, remote_name)
    client.fput_object(bucket, object_name, str(local_file))
    print(f"Uploaded DDL: s3a://{bucket}/{object_name}")


def run_generator(repo_root: Path, scenario: str) -> None:
    script = repo_root / "pyspark-database" / "scenarios" / scenario / "generate_source.py"
    if not script.is_file():
        raise FileNotFoundError(f"Missing generator: {script}")
    print(f"Generating {scenario}")
    subprocess.run([sys.executable, str(script)], cwd=repo_root, check=True)


def stage_ddl(repo_root: Path) -> Path:
    source = repo_root / "pyspark-database" / "sql" / "01_schema.sql"
    target = repo_root / "data" / "database_scenarios" / "DDL" / "ddl.sql"
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source, target)
    return target


def main() -> None:
    args = parse_args()
    if not args.skip_generate and importlib.util.find_spec("pyarrow") is None:
        raise SystemExit(
            "Missing Python package: pyarrow\n\n"
            "The publisher generates Parquet files, so install the local publishing dependencies first:\n\n"
            "  C:\\Python311\\python.exe -m pip install --user minio pyarrow\n"
        )
    repo_root = Path(__file__).resolve().parents[2]
    selected = [*SCENARIOS, *(OPTIONAL_SCENARIOS if args.include_heavy else [])]

    if not args.skip_generate:
        for scenario in selected:
            run_generator(repo_root, scenario)

    ddl_file = stage_ddl(repo_root)
    client = minio_client(args.endpoint, args.access_key, args.secret_key)
    ensure_bucket(client, args.bucket)

    upload_file(client, args.bucket, ddl_file, "DDL/ddl.sql", args.prefix)
    data_root = repo_root / "data" / "database_scenarios"
    for scenario in selected:
        print(f"Uploading {scenario}")
        upload_directory(client, args.bucket, data_root / scenario, scenario, args.prefix)

    print(f"Published lab root: s3a://{args.bucket}/{args.prefix.strip('/')}".rstrip("/") + "/")


if __name__ == "__main__":
    main()
