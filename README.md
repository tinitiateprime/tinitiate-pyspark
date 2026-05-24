![Pyspark Tinitiate Image](tinitiate-pyspark.png)
# PySpark Training
> Venkata Bhattaram &copy; TINITIATE.COM

## PySpark Local Setup

## 🔰 PySpark Basics
* Introduction to Apache Spark & PySpark
* Installing and configuring PySpark (local & cluster mode)
* SparkSession and SparkContext
* RDD vs DataFrame vs Dataset
* Basic DataFrame operations (select, filter, withColumn, etc.)
* Schema definition and inference
* User-Defined Functions (UDFs) and SQL functions
* Caching and persisting data (cache(), persist())

## 📥 Reading and Writing Data
* File Formats
* Reading CSV files (header, schema inference, multi-line, custom delimiter)
* Reading JSON files (multiline, nested JSON)
* Reading Parquet, ORC, and Avro
* Writing data in CSV, JSON, Parquet formats
* Handling corrupt records and bad data during ingestion
* Databases
* Reading from PostgreSQL/MySQL using JDBC
* Writing to PostgreSQL/MySQL (batch mode, truncate-load, upsert)
* Handling result partitioning for JDBC reads
* Pushdown predicates for predicate filtering in DB reads
* Cloud Storage
* Connecting to AWS S3 (using Hadoop, AWS keys, instance profiles)
* Reading and writing data from S3 buckets
* Using S3 Select with PySpark for efficient reads

## 🔄 Transformations and Actions
* Narrow vs Wide Transformations
* Map, Filter, GroupBy, Join operations
* Pivoting and unpivoting data
* Window functions (ranking, cumulative sums, lag/lead)
* Aggregations and analytics functions

## 🧱 Partitioning and Optimization
* Repartitioning vs Coalesce – when and how to use
* Bucketing and partitioning data in storage for faster reads
* Broadcast variables and broadcast joins
* Avoiding shuffles and skew in Spark jobs
* Catalyst optimizer and Tungsten execution engine
* Query execution and logical vs physical plans (explain())

## 📝 Handling File-based Updates
* Insert, update, delete patterns for data in files (SCD Type 1/2)
* Overwrite vs Append modes
* Using Delta Lake or Apache Hudi for file updates and deletes
* Merge operations in Delta Lake/Hudi

## ⚡ Performance Tuning
* Spark Configuration tuning (executors, memory, cores)
* Handling small and large files (file size optimization)
* Adaptive Query Execution (AQE)
* Exploiting catalyst optimizer for custom logic
* Minimizing data serialization overhead
* Skew handling (salting, skew join techniques)
* Persisting and caching strategies

## 🧠 In-Memory Processing & Collecting
* In-memory caching and broadcasting objects
* Collecting and converting DataFrames to Pandas (toPandas)
* Using Arrow for faster data transfer

## 🧪 Unit Testing & Deployment
* Testing PySpark jobs with pytest
* Mocking Spark DataFrames
* Packaging PySpark jobs for deployment (wheel, jar, zip)
* Submitting Spark jobs (spark-submit) to production
* CI/CD considerations for PySpark projects

## ☁️ Apache Spark with Modern Data Engineering
* Delta Lake, Apache Hudi, Iceberg – modern data lake formats
* Structured Streaming with PySpark
* Streaming from Kafka / Kinesis
* Handling late data and watermarking
* Checkpointing and recovery strategies
* Real-time ETL architecture with PySpark

## 💡 Advanced Concepts
* Using MLlib with PySpark (machine learning basics)
* GraphFrames and graph processing
* Vectorized UDFs and pandas UDFs
* Spark SQL and views
* Tuning cost-based optimizer (CBO) and statistics

## 🔍 Debugging & Monitoring
* Spark UI: Understanding stages, tasks, DAG
* Logging in PySpark (log4j configs)
* Handling out-of-memory errors and stragglers
* Exception handling in PySpark jobs
