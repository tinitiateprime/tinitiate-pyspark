# PySpark with PostgreSQL and MinIO

[Home](README.md) | [Setup](SETUP.md) | [PySpark Basics](PYSPARK_BASICS.md) | [Data Lake](DATA_LAKE.md)

This section is the main database loading lab.

Students work through a practical data engineering flow: source files are stored in MinIO, PySpark reads those files, and the data is loaded into PostgreSQL tables. The lab covers CSV, JSON, and Parquet files, and shows students how to validate the loaded data using PostgreSQL or DBeaver.

Use the student guide first. It explains the exact Docker containers, MinIO bucket, PostgreSQL tables, commands, and validation steps.

| Topic | Link |
|---|---|
| Complete student guide: Docker, PostgreSQL, MinIO, DBeaver, PySpark load | [MinIO to PostgreSQL student guide](pyspark-database/MINIO_TO_POSTGRES_SCENARIOS.md) |
| Optional: generate source files locally | [Generate files locally](pyspark-database/GENERATE_FILES_LOCALLY.md) |
| Dataset notes | [Database datasets](pyspark-database/DATASETS.md) |
| Scenario list | [Database scenarios](pyspark-database/scenarios/README.md) |

After this lab, continue to [Data Lake](DATA_LAKE.md) or [Performance Tuning](PERFORMANCE_TUNING_INDEX.md).
