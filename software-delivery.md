# Software Delivery

Each section has one Docker Compose YAML file. Give the student the YAML file for the class they are taking, then run it from the folder where the YAML file exists.

```powershell
cd software_delivery_yamls
docker compose -f 03_pyspark_s3_db_nosql_airflow.yml up -d
```

The tutorial repositories are downloaded into `./notebooks/tutorials`. Jupyter uses token `admin123`.

All YAML files are in the `software_delivery_yamls` folder. Full setup details are in `software_delivery_yamls/README.md`.

## Software Installer
YAML file: `software_delivery_yamls/01_software_installer.yml`

This starts browser-based tools:
* VS Code in the browser: http://localhost:8444
* DBeaver CloudBeaver in the browser: http://localhost:8978
* Python and Git inside the `student-tools` container

Docker Desktop must already be installed on the student's computer because Docker Compose needs Docker to run.

## Python only
YAML file: `software_delivery_yamls/02_python_only.yml`

Services:
* Jupyter: http://localhost:8888
* Mysql DB
* Python tutorials from GitHub

## pyspark S3 DB NoSQL Airflow
YAML file: `software_delivery_yamls/03_pyspark_s3_db_nosql_airflow.yml`

Services:
* Jupyter: http://localhost:8888
* Spark master: http://localhost:8088
* Airflow: http://localhost:8081
* MinIO API: http://localhost:9000
* MinIO console: http://localhost:9001
* Postgres
* DynamoDB Local
* FTP Server

Tutorials:
* Postgres - https://github.com/tinitiateprime/postgresql.git
* Dynamo DB - https://github.com/tinitiateprime/aws-dynamo.git
* Minio - https://github.com/tinitiateprime/minio.git
* Pyspark - https://github.com/tinitiateprime/pyspark.git
* Airflow - https://github.com/tinitiateprime/aws-airflow.git
* FTP Server - https://github.com/tinitiateprime/ftp-server.git

## DB Only
YAML file: `software_delivery_yamls/04_db_only.yml`

Services:
* SQL Server
* DBeaver CloudBeaver in the browser: http://localhost:8978

Tutorials:
* SQL Server - https://github.com/tinitiateprime/sqlserver.git

## pyspark S3 DB NoSQL Airflow NiFi
YAML file: `software_delivery_yamls/05_pyspark_s3_db_nosql_airflow_nifi.yml`

Services:
* Jupyter: http://localhost:8888
* Spark master: http://localhost:8088
* Airflow: http://localhost:8081
* MinIO console: http://localhost:9001
* NiFi: https://localhost:8443
* Postgres
* DynamoDB Local
* FTP Server

Tutorials:
* Postgres - https://github.com/tinitiateprime/postgresql.git
* Dynamo DB - https://github.com/tinitiateprime/aws-dynamo.git
* Minio - https://github.com/tinitiateprime/minio.git
* Pyspark - https://github.com/tinitiateprime/pyspark.git
* Airflow - https://github.com/tinitiateprime/aws-airflow.git
* FTP Server - https://github.com/tinitiateprime/ftp-server.git
* NiFi - https://github.com/tinitiateprime/nifi.git

## pyspark S3 DB NoSQL Airflow Kafka
YAML file: `software_delivery_yamls/06_pyspark_s3_db_nosql_airflow_kafka.yml`

Services:
* Jupyter: http://localhost:8888
* Spark master: http://localhost:8088
* Airflow: http://localhost:8081
* MinIO console: http://localhost:9001
* Kafka UI: http://localhost:8080
* Postgres
* DynamoDB Local
* FTP Server

Tutorials:
* Postgres - https://github.com/tinitiateprime/postgresql.git
* Dynamo DB - https://github.com/tinitiateprime/aws-dynamo.git
* Minio - https://github.com/tinitiateprime/minio.git
* Pyspark - https://github.com/tinitiateprime/pyspark.git
* Airflow - https://github.com/tinitiateprime/aws-airflow.git
* FTP Server - https://github.com/tinitiateprime/ftp-server.git
* Kafka - https://github.com/tinitiateprime/kafka.git

## pyspark S3 DB NoSQL Airflow NiFi Kafka
YAML file: `software_delivery_yamls/07_pyspark_s3_db_nosql_airflow_nifi_kafka.yml`

Services:
* Jupyter: http://localhost:8888
* Spark master: http://localhost:8088
* Airflow: http://localhost:8081
* MinIO console: http://localhost:9001
* Kafka UI: http://localhost:8080
* NiFi: https://localhost:8443
* Postgres
* DynamoDB Local
* FTP Server

Tutorials:
* Postgres - https://github.com/tinitiateprime/postgresql.git
* Dynamo DB - https://github.com/tinitiateprime/aws-dynamo.git
* Minio - https://github.com/tinitiateprime/minio.git
* Pyspark - https://github.com/tinitiateprime/pyspark.git
* Airflow - https://github.com/tinitiateprime/aws-airflow.git
* FTP Server - https://github.com/tinitiateprime/ftp-server.git
* NiFi - https://github.com/tinitiateprime/nifi.git
* Kafka - https://github.com/tinitiateprime/kafka.git
