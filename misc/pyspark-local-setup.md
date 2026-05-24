![Pyspark Tinitiate Image](tinitiate-pyspark.png)
# PySpark Training
> Venkata Bhattaram &copy; TINITIATE.COM

* Local Setup
  * **STEP 1:** Make sure Docker Desktop is installed
  * **STEP 2:** Download the Docker Compose YML file `pyspark-docker-setup.yml`
  * **STEP 3:** In command prompt as Admin, run the command `docker-compose -f pyspark-docker-setup.yml up -d`
  * STEP 4: Docker for PySpark and Jupyter Notebook should be installed

`docker-compose -f ti-sqlserver-db-docker-compose.yml up -d
* Starting Notebooks
  * **STEP 1.** Make sure the PySpark/Jupyter is running in Docker
  * **STEP 2.** Get Notebook URL
    * In command prompt (as Admin) run
    ```bash
    docker logs jupyter
    ```
    * From here get the URL that looks like:
    `http://127.0.0.1:8888/?token=6dfbbd2d3b43b8dcaab5b123e12abef4e9bfcda16b90a95e`    
  * **STEP 3.** Run in browser
    * Paste that in browser and start creating notebooks
* Local Data access (Read/Write)
* Docker DB access
* Cloud Storage Access
* Cloud DB Access
* PySpark
