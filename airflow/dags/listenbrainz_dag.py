from datetime import datetime
import os
import logging
import csv
import json

from airflow import DAG
from airflow.decorators import task, task_group
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import List

# Global configuration constants
DUCKDB_PATH: str = "/data/listenbrainz.duckdb"  # Mounted path inside Docker
DB_URL: str = f"duckdb:///{DUCKDB_PATH}"          # SQLAlchemy DB URL for DuckDB
JSON_DIR: str = "/data/raw"                        # Directory containing JSON files
SCHEMA_SQL_PATH: str = "/opt/airflow/sql/create_schema.sql"  # SQL schema file
INTERMEDIATE_DIR: str = "/data/intermediate"       # Directory for intermediate CSV files

# Ensure the intermediate directory exists
os.makedirs(INTERMEDIATE_DIR, exist_ok=True)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

def get_csv_path(file_path: str) -> str:
    # Convert file_path to string in case it's a MappedArgument.
    file_path_str = str(file_path)
    base = os.path.basename(file_path_str)
    name, _ = os.path.splitext(base)
    return os.path.join(INTERMEDIATE_DIR, f"{name}.csv")

@task
def create_schema() -> None:
    """
    Create or verify the DuckDB schema by executing SQL commands from SCHEMA_SQL_PATH.
    """
    engine: Engine = create_engine(DB_URL, future=True)
    with engine.begin() as conn:
        with open(SCHEMA_SQL_PATH, "r") as f:
            schema_sql = f.read()
        conn.execute(text(schema_sql))
    logging.info("Schema created/ensured.")

@task_group(group_id="process_file")
def process_file_pipeline(file_path: str):
    """
    A TaskGroup that processes one JSON file through all pipeline steps:
      1. Parse the JSON file into a CSV.
      2. Load the CSV into the staging table.
      3. Deduplicate the staging table.
      4. Upsert dimension tables.
      5. Insert fact records.
    """
    csv_path = get_csv_path(file_path)

    @task(task_id="parse", pool="sequential_pool")
    def parse_task(file_path: str, csv_path: str) -> None:
        from src.pipeline.unified_pipeline import parse_json_to_csv
        parse_json_to_csv(file_path, csv_path)
        logging.info(f"Parsed {file_path} into {csv_path}.")

    @task(task_id="load", pool="sequential_pool")
    def load_task(csv_path: str) -> None:
        from src.pipeline.unified_pipeline import load_csv_to_stg
        engine = create_engine(DB_URL, future=True)
        load_csv_to_stg(engine, csv_path, "stg_listens", chunksize=50000)
        logging.info(f"Loaded CSV {csv_path} into staging table.")

    @task(task_id="dedup", pool="sequential_pool")
    def dedup_task() -> None:
        from src.pipeline.unified_pipeline import db_side_dedup
        engine = create_engine(DB_URL, future=True)
        db_side_dedup(engine, "stg_listens", "stg_listens_dedup")
        logging.info("Deduplicated staging data.")

    @task(task_id="upsert", pool="sequential_pool")
    def upsert_task() -> None:
        from src.pipeline.unified_pipeline import upsert_dimensions
        engine = create_engine(DB_URL, future=True)
        upsert_dimensions(engine)
        logging.info("Upserted dimension tables.")

    @task(task_id="insert_fact", pool="sequential_pool")
    def insert_fact_task() -> None:
        from src.pipeline.unified_pipeline import insert_fact_listen
        engine = create_engine(DB_URL, future=True)
        insert_fact_listen(engine)
        logging.info("Inserted fact records.")

    # Chain tasks sequentially within the TaskGroup.
    parsed = parse_task(file_path, csv_path)
    loaded = load_task(csv_path)
    deduped = dedup_task()
    upserted = upsert_task()
    facted = insert_fact_task()

    parsed >> loaded >> deduped >> upserted >> facted

# At DAG parse time, list all JSON files in JSON_DIR.
# (This is static. If you need dynamic behavior at runtime, you would need a different approach.)
files = [os.path.join(JSON_DIR, f) for f in os.listdir(JSON_DIR) if f.lower().endswith(".json")]
files = sorted(files)  # Optionally sort for predictable order

with DAG(
        dag_id="listenbrainz_dynamic_pipeline_sequential",
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        is_paused_upon_creation=True
) as dag:
    schema = create_schema()

    # Build TaskGroups sequentially for each file.
    previous_group = None
    for file_path in files:
        current_group = process_file_pipeline(file_path)
        if previous_group:
            previous_group >> current_group
        else:
            # Ensure schema creation is done before the first file is processed.
            schema >> current_group
        previous_group = current_group
