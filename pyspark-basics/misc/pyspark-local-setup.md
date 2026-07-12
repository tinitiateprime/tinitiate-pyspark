# PySpark Local Setup

Use this page to run PySpark locally with Docker.

This setup uses the same Docker stack used in the PySpark to PostgreSQL lab.

It starts:

- PostgreSQL
- MinIO
- PySpark / Jupyter
- Spark master and worker

## STEP 1: Install Docker Desktop

Download Docker Desktop for Windows:

<https://www.docker.com/products/docker-desktop/>

Install Docker Desktop, start it, and wait until Docker says it is running.

Check Docker:

```cmd
docker version
docker ps
```

If Docker is installed correctly, these commands should run without errors.

## STEP 2: Download and extract this project

Download the project files and extract them into:

```text
C:\tinitiate_pyspark
```

Then open Command Prompt and move into the project folder:

```cmd
cd C:\tinitiate_pyspark
```

This folder is the repository root. Run the remaining commands from this folder.

## STEP 3: Start the Docker stack

Run:

```cmd
docker compose -f pyspark-database/ti-data-engineering-docker-compose.yml up -d
```

This starts the containers needed for the lab.

For the beginner labs, focus on these:

| Service | Container | URL or port |
|---|---|---|
| PostgreSQL | `postgres` | `localhost:5432` |
| MinIO | `minio` | API: `localhost:9000`, browser: `http://localhost:9001` |
| Jupyter | `jupyter` | `http://localhost:8888` |

Check containers:

```cmd
docker ps
```

You should see containers such as:

```text
postgres
minio
jupyter
spark-master
spark-worker
```

## STEP 4: Open Jupyter Notebook

Make sure the Jupyter container is running:

```cmd
docker ps
```

Get the Jupyter URL:

```cmd
docker logs jupyter
```

Look for a URL like this:

```text
http://127.0.0.1:8888/lab?token=...
```

Copy that URL and paste it into your browser.

If the URL asks for a token, use the token from the Docker logs.

## STEP 5: Access local notebooks and files

The Jupyter container opens inside the Docker environment.

Use it to run the PySpark notebooks from this repository.

Common folders:

```text
pyspark-basics
pyspark-notebooks
pyspark-database
pyspark-datalake
```

## STEP 6: PostgreSQL access

PostgreSQL runs in Docker.

Connection details:

```text
Host: localhost
Port: 5432
Database: tinitiateai
User: ti_dbuser
Password: tiuser!23456
```

You can connect using DBeaver or from Docker commands.

Example:

```cmd
docker exec -e PGPASSWORD=tiuser!23456 postgres psql -U ti_dbuser -d tinitiateai -c "\dt"
```

## STEP 7: MinIO access

Open MinIO in the browser:

```text
http://localhost:9001
```

Login:

```text
Username: minio
Password: minio123
```

The lab bucket is:

```text
datalake
```

## STEP 8: PySpark to PostgreSQL lab

After Docker, PostgreSQL, MinIO, and Jupyter are running, continue here:

[`../../pyspark-database/MINIO_TO_POSTGRES_SCENARIOS.md`](../../pyspark-database/MINIO_TO_POSTGRES_SCENARIOS.md)

That guide explains how to:

1. create PostgreSQL tables;
2. upload source files to MinIO;
3. load CSV, JSON, or Parquet files from MinIO into PostgreSQL using PySpark.
