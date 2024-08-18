# Overview
This document outlines a data pipeline example for web scraping 🕸️🌐  
The key feature of this pipeline is the **parallelization** of the extraction tasks, applying the MapReduce pattern  🗺️📉  
The map step leverages **dynamic parameters** to distribute the workload across multiple tasks, thereby accelerating the web scraping process ⚡🚀

## Solution
- **Airflow**: for task orchestration, utilizing the TaskFlow API
- **MinIO**: a local S3 storage for intermediate results, where JSON files from each extract task instance are stored. Alternatively, AWS S3 can be used since the pipeline leverages the Airflow's S3 hook
- **PostgreSQL**: a Data Warehouse to store the extracted job data



## Dynamic Task Mapping
Airflow supports dynamic task generation.  
In this example, the DAG consists of 4 steps, with the `extract_jobs` task dynamically mapped into multiple parallel tasks.

### DAG tasks
![DAG tasks](docs/images/DAG_tasks.png)
- init
- map_search_tasks - to split the input variables ()

### MapReduce visualization
![MapReduce](docs/images/MapReduce.svg)

## How to Use
1. Rename `*.env.example*` to `*.env*` and adjust the parameters as needed.
2. Run `docker compose up`.
3. Access the Airflow UI (default URL: http://localhost:8080/).
4. Configure the following connections under *Admin -> Connections*:
   - PostgreSQL:
        - Connection Id: *postgres_conn*
        - Connection Type: *Postgres*
        - Host: *postgres*
        - Database: *airflow*
        - Login and Password: *airflow*
        - Port: *5432*
   - MinIO:
        - Connection Id: *s3_conn*
        - Connection Type: *Amazon Web Services*
        - AWS Access Key ID and Secret Access Key: *minioadmin*
        - Extra: *{ "endpoint_url": "http://minio:9000" }*
5. Set variables in *Admin -> Variables*  
For example:
    ```
    {
        "locations": ["Berlin", "Munich"]
        "keywords_list": ["data", "cloud"],
    }
    ```
6. Trigger the `find_jobs` DAG.