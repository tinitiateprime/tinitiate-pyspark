# Data Lake

[Home](README.md) | [PySpark with PostgreSQL and MinIO](PYSPARK_POSTGRES_MINIO.md) | [Performance Tuning](PERFORMANCE_TUNING_INDEX.md)

This section explains data lake style processing with PySpark, MinIO, and Apache Iceberg.

Students first use the same source files that were created for the PostgreSQL lab. Those files are stored in the MinIO `datalake` bucket. Then PySpark reads the files and creates Iceberg tables in two layers:

- Bronze layer: raw data loaded from the source files.
- Silver layer: cleaned data created from the Bronze layer.

Use the Iceberg Bronze/Silver lab first. The older MinIO batch topics are also available for extra practice.

| Topic | Link |
|---|---|
| Iceberg Bronze and Silver layers | [Iceberg Bronze/Silver lab](pyspark-datalake/ICEBERG_BRONZE_SILVER.md) |
| Dataset design | [Dataset design](pyspark-datalake/01_dataset_design/README.md) |
| MinIO setup | [MinIO setup](pyspark-datalake/02_minio_setup/README.md) |
| Batch loads | [Batch loads](pyspark-datalake/03_batch_loads/README.md) |
| Compaction | [Small-file compaction](pyspark-datalake/04_compaction/README.md) |
| Validation | [Validation](pyspark-datalake/05_validation/README.md) |
| End-to-end MinIO batch tutorial | [Batch loads with MinIO](pyspark-datalake/docs/batch-loads-minio.md) |

After this section, continue to [Performance Tuning](PERFORMANCE_TUNING_INDEX.md).
