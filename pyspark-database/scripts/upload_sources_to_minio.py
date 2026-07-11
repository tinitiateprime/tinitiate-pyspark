#!/usr/bin/env python3
"""Upload one generated database scenario directory to a stable MinIO prefix."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

try:
    from minio import Minio
except ModuleNotFoundError as exc:
    raise SystemExit(
        "Missing Python package: minio\n\n"
        "Install the MinIO uploader dependency with the same Python used to run this script:\n\n"
        "  C:\\Python311\\python.exe -m pip install --user minio\n\n"
        "If you also need local PySpark and PyArrow, install the full requirements later:\n\n"
        "  C:\\Python311\\python.exe -m pip install --user -r pyspark-database/requirements.txt\n\n"
        "Then run the upload again, for example:\n\n"
        "  C:\\Python311\\python.exe pyspark-database/scripts/upload_sources_to_minio.py "
        "--source-dir data/database_scenarios/01_many_small_json_customer "
        "--scenario 01_many_small_json_customer"
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:

  Upload Scenario 01:
    python pyspark-database/scripts/upload_sources_to_minio.py --source-dir data/database_scenarios/01_many_small_json_customer --scenario 01_many_small_json_customer

  Upload Scenario 07:
    python pyspark-database/scripts/upload_sources_to_minio.py --source-dir data/database_scenarios/07_many_small_parquet_transaction --scenario 07_many_small_parquet_transaction
""",
    )
    parser.add_argument("--source-dir", required=True, help="Local scenario output root.")
    parser.add_argument("--scenario", required=True, help="MinIO folder name, for example 01_many_small_json_customer.")
    parser.add_argument("--bucket", default=os.environ.get("MINIO_BUCKET", "datalake"))
    parser.add_argument("--prefix", default="", help="Optional MinIO prefix. Leave empty to upload directly under the bucket root.")
    parser.add_argument("--endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minio"))
    parser.add_argument("--secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minio123"))
    if len(sys.argv) == 1:
        parser.print_help()
        raise SystemExit(
            "\nMissing required inputs.\n\n"
            "First run a scenario generator, for example:\n"
            "  python pyspark-database/scenarios/01_many_small_json_customer/generate_source.py\n\n"
            "Then upload that generated scenario folder:\n"
            "  python pyspark-database/scripts/upload_sources_to_minio.py "
            "--source-dir data/database_scenarios/01_many_small_json_customer "
            "--scenario 01_many_small_json_customer"
        )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = Path(args.source_dir).resolve()
    if not source.is_dir():
        raise FileNotFoundError(f"Source directory does not exist: {source}")

    endpoint = urlparse(args.endpoint)
    if not endpoint.hostname:
        raise ValueError("--endpoint must include http:// or https://")
    host = endpoint.hostname if endpoint.port is None else f"{endpoint.hostname}:{endpoint.port}"
    client = Minio(host, access_key=args.access_key, secret_key=args.secret_key, secure=endpoint.scheme == "https")
    if not client.bucket_exists(args.bucket):
        client.make_bucket(args.bucket)

    files = sorted(path for path in source.rglob("*") if path.is_file())
    if not files:
        raise ValueError(f"No files found below {source}")
    prefix = args.prefix.strip("/")
    scenario = args.scenario.strip("/")
    root_prefix = f"{prefix}/{scenario}" if prefix else scenario
    for number, path in enumerate(files, 1):
        object_name = f"{root_prefix}/{path.relative_to(source).as_posix()}"
        client.fput_object(args.bucket, object_name, str(path))
        if number == 1 or number % 1000 == 0 or number == len(files):
            print(f"Uploaded {number:,}/{len(files):,}: s3a://{args.bucket}/{object_name}")

    print(f"Student root: s3a://{args.bucket}/{root_prefix}/")


if __name__ == "__main__":
    main()
