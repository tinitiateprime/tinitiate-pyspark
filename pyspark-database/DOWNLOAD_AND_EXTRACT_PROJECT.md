# Download and Extract the Lab Project

Use this page before starting the MinIO to PostgreSQL lab.

Students need the lab project files on their computer before running Docker, Python, MinIO, or PySpark commands.

## Option 1: Download the ZIP from GitHub

Go to this GitHub page:

<https://github.com/tinitiateprime/data-appliance/blob/main/README.md>

Download the ZIP file provided there.

After downloading, extract the ZIP file into:

```text
C:\tinitiate_pyspark
```

After extraction, the folder should look like this:

```text
C:\
  tinitiate_pyspark
    README.md
    pyspark-database
      MINIO_TO_POSTGRES_SCENARIOS.md
      scripts
      scenarios
      sql
```

## Option 2: Use a ZIP file shared by the instructor

If the instructor gives the ZIP file directly, download it first.

Then extract it into:

```text
C:\tinitiate_pyspark
```

Important: make sure the final project folder is:

```text
C:\tinitiate_pyspark
```

Do not keep the files inside an extra nested folder like this:

```text
C:\tinitiate_pyspark\tinitiate_pyspark
```

If that happens, move the inner files up one level so the project files are directly inside:

```text
C:\tinitiate_pyspark
```

## Verify the project folder

Open Command Prompt and run:

```cmd
cd C:\tinitiate_pyspark
dir
```

You should see folders/files such as:

```text
README.md
pyspark-database
```

If you can see `pyspark-database`, the project files are in the correct location.

Now return to the main lab guide:

[`MINIO_TO_POSTGRES_SCENARIOS.md`](MINIO_TO_POSTGRES_SCENARIOS.md)
