# ListenBrainz Pipeline

This repository contains a scalable data ingestion pipeline for ListenBrainz data using **Apache Airflow** and **DuckDB**. You can run the pipeline either via the **Airflow UI** (for visual monitoring) or directly via the **Python main file**.

---

## Table of Contents

- [Requirements](#requirements)
- [Setup and Running the Pipeline via Airflow](#setup-and-running-the-pipeline-via-airflow)
- [Running the Pipeline via Python Main File](#running-the-pipeline-via-python-main-file)
- [Exploring the Database](#exploring-the-database)
- [Notes](#notes)
- [Data Analysis](#analysis)

---

## Requirements

- **Docker** (on Linux or macOS)
- **(Optional) Airflow installed via Docker Compose**
- A tool to view DuckDB files (e.g., [DBeaver](https://dbeaver.io/))
- Your **ListenBrainz** data files (in `.json` format)

---

## Setup and Running the Pipeline via Airflow

Follow these steps to run the pipeline using **Airflow** and check the progress visually:

### **1. Install Docker**

- Install Docker on your **Linux** or **macOS** machine.
- Ensure your user has the appropriate permissions to run Docker commands.

### **2. Place Your Data Files**
```bash
cd data_pipeline/airflow
mkdir data logs plugins
mkdir data/raw
```
- Copy your `.json` data file(s) into the `airflow/data/raw` directory. Create 'raw' directory if not existing.
- Ensure the file names end with the `.json` extension.

### **3. Build and Run Airflow**

Run the following commands in your terminal:

```bash
chmod -R 777 data/ logs/ dags/ plugins/
cd ..
docker-compose -f airflow/docker-compose.yaml build
docker-compose -f airflow/docker-compose.yaml up
```

### **4. Access the Airflow UI**

- Wait for the Airflow webserver to start.
- Open your browser and go to `http://localhost:8080` (or the port Airflow is running on).
- Log in with:
  - **Username:** `admin`
  - **Password:** `admin`

### **5. Run the DAG**

- Navigate to the **DAGs** section in the Airflow UI.
- Find the DAG named **"listenbrainz\_dag"**.
- Trigger the DAG to start processing the files.

### **6. Monitor the Pipeline**

- Click on the **Graph View** to see each file being processed.
- Each block represents a file, and inside each block are multiple pipeline stages.
- Wait for the entire pipeline to complete.

### **7. Explore the Database**

- After completion, a new database file `listenbrainz.duckdb` will appear in `airflow/data/`.
- Open this file using a database viewer like **DBeaver** to explore the tables.

---

## Running the Pipeline via Python Main File

If you prefer to run the pipeline **without Airflow**, you can execute the main Python file:

### **1. Place Your Data Files**
```bash
cd data_pipeline
mkdir data
mkdir data/raw
```
- Move your `.json` file(s) into the `data/raw` directory.


### **2. install requirements**
- Create a virtual python env or use an existing one. Activate the env.

Run the following command:
```bash
pip install -r requirements.txt
```

### **3. Run the Pipeline**

Run the following command in your terminal:

```bash
python src/pipeline/unified_pipeline.py --json-dir data/raw/
```

### **4. Monitor the Process**

- The script will process each JSON file and create the **DuckDB** database.
- Once finished, the file `listenbrainz.duckdb` will be available in `data/`.

### **5. Explore the Database**

- Open the `listenbrainz.duckdb` file using a **database viewer** like [DBeaver](https://dbeaver.io/).

---

## Exploring the Database

After running the pipeline (using either method), you'll have a DuckDB database file (`listenbrainz.duckdb`) containing:

### **Tables Included:**

- **Dimension Tables:** `dim_user`, `dim_track`
- **Fact Table:** `fact_listen`
- **Staging Tables:** `stg_listens`, `stg_listens_dedup`

To inspect and query the data, open the **DuckDB file** using a tool like **DBeaver**.

---

## Notes

- **Mac Issues:**
  - If you are unable to run our airflow docker service on mac machines due to permisison issues or status 132 exit during airflow-db init. Please use the python script. I do not have a mac machine, and I could not test it. I tried testing it on github mac runner, but was not able to do it due to architectural compatibility issues.
- **Permissions Issues:**

  - If you face permission errors with mounted volumes, use:
    ```bash
    chmod -R 777 data/ logs/ dags/ plugins/
    ```

- **File Naming:**

  - Only files with the extension .json in `data/raw/` will be processed. You just need to rename your data files to add the json extension.

- **Airflow DAG Graph:**

  - The DAG graph will show **each file as a separate block**.
  - Inside each block, multiple pipeline **stages** will be displayed.

- **Running in a Cloud/CI/CD Environment:**

  - Modify paths and configurations as needed.
  - Ensure **Docker** is properly set up.

---

## Analysis
- Go to data_pipeline/sql/queries folder.
- Here you will find test_queries.sql for all 5 queries. You may run these queries in dbeaver/something-else with the produced duckdb file to see the results.
- You may also check the results of these queries in query_results.txt file.
- Additionally, you can also run offline_queries.py in data_pipeline/scripts folder to get the results with use of python logic to compare them against the DB results for validity.