import csv
import datetime
import json
import logging
import os
import pandas as pd
from typing import Dict, Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Configure logging for info messages
logging.basicConfig(level=logging.INFO)

# Global configuration constants
CHECKPOINT_FILE: str = "pipeline_checkpoint.json"  # For storing stage pipeline progress if run as a python program
SCHEMA_SQL_PATH: str = os.path.join("sql", "create_schema.sql")
CSV_TEMP_FILE: str = "temp_listens.csv"
JSON_DATA_FILE_NAME = "sample-data.json"  # Default file name if needed

# =============================================================================
# Utility Functions
# =============================================================================
def epoch_to_timestamp_str(epoch_time: float) -> str:
    """
    Convert an epoch time (seconds) to a standard timestamp string.

    The format is 'YYYY-MM-DD HH:MM:SS', which is compatible with both DuckDB and Postgres.

    Args:
        epoch_time (float): Epoch time in seconds.

    Returns:
        str: Formatted timestamp string.
    """
    dt: datetime.datetime = datetime.datetime.utcfromtimestamp(epoch_time)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def parse_json_to_csv(json_file: str, csv_file: str) -> None:
    """
    Parse newline-delimited JSON records and write selected fields to a CSV file.

    For each JSON record, this function:
      - Checks that required keys exist.
      - Converts the numeric 'listened_at' epoch to a timestamp string.
      - Normalizes text fields (track_name, artist_name) by trimming and lowercasing.
      - Writes the output with columns: user_id, track_id, track_name, artist_name, listened_at.

    Args:
        json_file (str): Path to the input JSON file.
        csv_file (str): Path to the output CSV file.
    """
    fieldnames = ["user_id", "track_id", "track_name", "artist_name", "listened_at"]

    with open(json_file, 'r', encoding='utf-8') as infile, \
         open(csv_file, 'w', encoding='utf-8', newline='') as outfile:

        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        count: int = 0
        skipped: int = 0

        # Process each line in the JSON file
        for line_num, line in enumerate(infile, start=1):
            try:
                record: Dict[str, Any] = json.loads(line)
                # Ensure required fields exist
                if not record.get("user_name") or not record.get("recording_msid") or not record.get("listened_at"):
                    raise ValueError("Missing required fields: user_name, recording_msid, or listened_at")

                # Convert epoch to timestamp string
                epoch_val: float = float(record["listened_at"])
                ts_str: str = epoch_to_timestamp_str(epoch_val)

                # Extract and normalize track metadata
                track_metadata: Dict[str, Any] = record.get("track_metadata", {})
                track_name: str = track_metadata.get("track_name", "").strip().lower()
                artist_name: str = track_metadata.get("artist_name", "").strip().lower()

                # Create a row for the CSV output
                row: Dict[str, str] = {
                    "user_id": record["user_name"],
                    "track_id": record["recording_msid"],
                    "track_name": track_name,
                    "artist_name": artist_name,
                    "listened_at": ts_str
                }
                writer.writerow(row)
                count += 1

            except Exception as e:
                logging.warning(f"Skipping line {line_num}: {e}")
                skipped += 1

        logging.info(f"parse_json_to_csv done. Wrote {count} rows, skipped {skipped}.")


def load_csv_to_stg(db_engine: Engine, csv_file: str, stg_table: str = "stg_listens", chunksize: int = 50000) -> None:
    """
    Load CSV data into a staging table in the database in chunks.

    Uses pandas to read the CSV file in chunks to handle large files and writes each chunk
    into the specified staging table.

    Args:
        db_engine (Engine): SQLAlchemy database engine.
        csv_file (str): Path to the CSV file.
        stg_table (str): Name of the staging table.
        chunksize (int): Number of rows per chunk.
    """
    total_rows: int = 0
    # Read CSV in chunks and parse "listened_at" as a datetime field
    chunk_iter = pd.read_csv(csv_file, chunksize=chunksize, parse_dates=["listened_at"])
    first_chunk: bool = True

    for i, chunk in enumerate(chunk_iter):
        rows_in_chunk: int = len(chunk)
        if first_chunk:
            chunk.to_sql(stg_table, db_engine, if_exists='replace', index=False)
            first_chunk = False
        else:
            chunk.to_sql(stg_table, db_engine, if_exists='append', index=False)
        total_rows += rows_in_chunk
        logging.info(f"Chunk {i + 1}: inserted {rows_in_chunk} rows into {stg_table}, total {total_rows}.")

    logging.info(f"All chunks inserted into {stg_table}. Total {total_rows} rows.")


def db_side_dedup(db_engine: Engine, stg_table: str = "stg_listens", stg_dedup: str = "stg_listens_dedup") -> None:
    """
    Deduplicate records in the staging table at the database level.

    Creates a new table (stg_listens_dedup) with distinct records based on:
      user_id, track_id, track_name, artist_name, and listened_at.

    Args:
        db_engine (Engine): SQLAlchemy database engine.
        stg_table (str): Original staging table name.
        stg_dedup (str): Name for the deduplicated table.
    """
    with db_engine.begin() as conn:
        logging.info("Starting deduplication process.")
        # Drop deduplication table if it exists
        conn.execute(text(f"DROP TABLE IF EXISTS {stg_dedup}"))
        # Create deduplicated table with distinct records
        conn.execute(text(f"""
            CREATE TABLE {stg_dedup} AS
            SELECT DISTINCT
                user_id,
                track_id,
                track_name,
                artist_name,
                listened_at
            FROM {stg_table}
            WHERE user_id IS NOT NULL
              AND track_id IS NOT NULL
              AND listened_at IS NOT NULL;
        """))
    logging.info(f"deduplicate: created {stg_dedup} from {stg_table} in DB.")


def upsert_dimensions(db_engine: Engine) -> None:
    """
    Upsert dimension tables for users and tracks.

    Inserts new records into dim_user and dim_track using a row_number approach to
    generate surrogate keys. The process uses window functions to determine new keys based
    on the existing maximum surrogate key in each dimension table.

    Args:
        db_engine (Engine): SQLAlchemy database engine.
    """
    with db_engine.begin() as conn:
        # Upsert into dim_user table
        conn.execute(text("""
        INSERT INTO dim_user (user_sk, user_id)
        WITH new_users AS (
            SELECT DISTINCT user_id FROM stg_listens_dedup
        ),
        existing_max AS (
            SELECT COALESCE(MAX(user_sk), 0) AS max_sk FROM dim_user
        ),
        to_insert AS (
            SELECT
              (row_number() OVER (ORDER BY nu.user_id) + em.max_sk) AS new_sk,
               nu.user_id
            FROM new_users nu
            LEFT JOIN dim_user du ON nu.user_id = du.user_id
            CROSS JOIN existing_max em
            WHERE du.user_id IS NULL
        )
        SELECT new_sk, user_id FROM to_insert;
        """))

        # Upsert into dim_track table
        conn.execute(text("""
        INSERT INTO dim_track (track_sk, track_id, track_name, artist_name)
        WITH new_tracks AS (
            SELECT DISTINCT track_id, track_name, artist_name
            FROM stg_listens_dedup
        ),
        existing_max AS (
            SELECT COALESCE(MAX(track_sk), 0) AS max_sk FROM dim_track
        ),
        to_insert AS (
            SELECT
              (row_number() OVER (ORDER BY nt.track_id) + em.max_sk) AS new_sk,
               nt.track_id,
               nt.track_name,
               nt.artist_name
            FROM new_tracks nt
            LEFT JOIN dim_track dt ON nt.track_id = dt.track_id
            CROSS JOIN existing_max em
            WHERE dt.track_id IS NULL
        )
        SELECT new_sk, track_id, track_name, artist_name FROM to_insert;
        """))
    logging.info("Upserted dim_user and dim_track tables.")


def insert_fact_listen(db_engine: Engine) -> None:
    """
    Insert new listen records into the fact_listen table.

    Joins the deduplicated staging table with the dimension tables to map natural keys
    (user_id and track_id) to surrogate keys (user_sk and track_sk). New records are inserted
    with a generated listen_sk, while ensuring that duplicate listens are skipped.

    Args:
        db_engine (Engine): SQLAlchemy database engine.
    """
    with db_engine.begin() as conn:
        conn.execute(text("""
        INSERT INTO fact_listen (listen_sk, user_sk, track_sk, listened_at)
        WITH existing_max AS (
            SELECT COALESCE(MAX(listen_sk), 0) AS max_sk FROM fact_listen
        ),
        joined AS (
            SELECT
                d.user_id,
                d.track_id,
                d.listened_at,
                du.user_sk,
                dt.track_sk
            FROM stg_listens_dedup d
            JOIN dim_user du ON d.user_id = du.user_id
            JOIN dim_track dt ON d.track_id = dt.track_id
        ),
        new_rows AS (
            SELECT
                (row_number() OVER (ORDER BY j.user_id, j.listened_at) + em.max_sk) AS new_listen_sk,
                j.user_sk,
                j.track_sk,
                j.listened_at
            FROM joined j
            CROSS JOIN existing_max em
            LEFT JOIN fact_listen f
                   ON f.user_sk = j.user_sk
                  AND f.track_sk = j.track_sk
                  AND f.listened_at = j.listened_at
            WHERE f.listen_sk IS NULL
        )
        SELECT new_listen_sk, user_sk, track_sk, listened_at
        FROM new_rows;
        """))
    logging.info("Inserted new listens into fact_listen.")


def load_checkpoint() -> Dict[str, bool]:
    """
    Load pipeline checkpoint data from a JSON file if it exists.

    Returns:
        Dict[str, bool]: A dictionary with completed stages as keys.
    """
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_checkpoint(stage: str) -> None:
    """
    Save a checkpoint for a completed pipeline stage.

    Args:
        stage (str): The stage name to mark as completed.
    """
    cp: Dict[str, bool] = load_checkpoint()
    cp[stage] = True
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(cp, f)


def main_pipeline(db_url: str, json_file: str) -> None:
    """
    Execute the main data pipeline for a single JSON file.

    The pipeline performs the following steps:
      1. Create or verify the database schema.
      2. Parse the JSON file to CSV.
      3. Load the CSV into a staging table.
      4. Deduplicate the staging data.
      5. Upsert dimension tables (dim_user, dim_track).
      6. Insert fact records (fact_listen).

    Args:
        db_url (str): SQLAlchemy database URL (e.g., for DuckDB or Postgres).
        json_file (str): Path to the input ListenBrainz JSON file.
    """
    engine: Engine = create_engine(db_url, future=True)
    checkpoint: Dict[str, bool] = load_checkpoint()

    # Step 1: Create schema if not already done.
    if not checkpoint.get("schema_created", False):
        with engine.begin() as conn:
            create_sql: str = open(SCHEMA_SQL_PATH, 'r', encoding='utf-8').read()
            conn.execute(text(create_sql))
        logging.info("Schema created/ensured.")
        save_checkpoint("schema_created")
    else:
        logging.info("Skipping schema creation (already done).")

    # Step 2: Parse JSON to CSV.
    if not checkpoint.get("csv_parsed", False):
        parse_json_to_csv(json_file, CSV_TEMP_FILE)
        logging.info("Parsed JSON -> CSV.")
        save_checkpoint("csv_parsed")
    else:
        logging.info("Skipping JSON parsing (already done).")

    # Step 3: Load CSV into staging table.
    if not checkpoint.get("csv_loaded", False):
        load_csv_to_stg(engine, CSV_TEMP_FILE, "stg_listens", chunksize=50000)
        logging.info("CSV loaded into stg_listens.")
        save_checkpoint("csv_loaded")
    else:
        logging.info("Skipping CSV load (already done).")

    # Step 4: Deduplicate staging table.
    if not checkpoint.get("deduplicated", False):
        db_side_dedup(engine, "stg_listens", "stg_listens_dedup")
        logging.info("Deduplicated staging.")
        save_checkpoint("deduplicated")
    else:
        logging.info("Skipping deduplication (already done).")

    # Step 5: Upsert dimension tables.
    if not checkpoint.get("dims_upserted", False):
        upsert_dimensions(engine)
        logging.info("Dimension tables upserted.")
        save_checkpoint("dims_upserted")
    else:
        logging.info("Skipping dimension upsert (already done).")

    # Step 6: Insert fact records.
    if not checkpoint.get("facts_inserted", False):
        insert_fact_listen(engine)
        logging.info("Fact records inserted.")
        save_checkpoint("facts_inserted")
    else:
        logging.info("Skipping fact insertion (already done).")

    logging.info("Pipeline complete!")


def main_pipeline_dir(db_url: str, json_dir: str) -> None:
    """
    Execute the data pipeline for all JSON files in a given directory.

    This function performs the following:
      1. Create or verify the database schema (once).
      2. Iterate over each JSON file (e.g., with a '.json' extension) in the directory.
      3. For each file, parse JSON to CSV, load the CSV into a staging table, deduplicate,
         upsert dimension tables, and insert fact records.
    Args:
        db_url (str): SQLAlchemy database URL.
        json_dir (str): Directory path containing JSON files.
    """
    engine: Engine = create_engine(db_url, future=True)

    # Create or verify schema (only once)
    with engine.begin() as conn:
        create_sql: str = open(SCHEMA_SQL_PATH, 'r', encoding='utf-8').read()
        conn.execute(text(create_sql))
    logging.info("Schema created/ensured for directory processing.")

    # Iterate over all JSON files in the directory.
    for root, _, files in os.walk(json_dir):
        for filename in files:
            if filename.lower().endswith('.json'):
                json_file: str = os.path.join(root, filename)
                logging.info(f"Processing file: {json_file}")
                # For each file, run the pipeline steps.
                parse_json_to_csv(json_file, CSV_TEMP_FILE)
                load_csv_to_stg(engine, CSV_TEMP_FILE, "stg_listens", chunksize=50000)
                db_side_dedup(engine, "stg_listens", "stg_listens_dedup")
                upsert_dimensions(engine)
                insert_fact_listen(engine)
    logging.info("All JSON files processed. Final facts table is complete.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--db-url", default=None,
                        help="SQLAlchemy DB URL (DuckDB or Postgres, etc.).")
    parser.add_argument("--json-file", default=None,
                        help="Path to a single ListenBrainz JSON file.")
    parser.add_argument("--json-dir", default=None,
                        help="Path to a directory containing multiple ListenBrainz JSON files.")
    args = parser.parse_args()

    # Use provided DB URL or default to local DuckDB
    if args.db_url is not None:
        db_url = args.db_url
    else:
        DEFAULT_DUCKDB_DIR: str = os.path.join(os.getcwd(), "data", "duck_db")
        if not os.path.exists(DEFAULT_DUCKDB_DIR):
            os.makedirs(DEFAULT_DUCKDB_DIR)
        DEFAULT_DUCKDB_PATH: str = os.path.join(DEFAULT_DUCKDB_DIR, "listenbrainz.duckdb")
        db_url = f"duckdb:///{DEFAULT_DUCKDB_PATH}"

    if args.json_dir:
        main_pipeline_dir(db_url, args.json_dir)
    elif args.json_file:
        main_pipeline(db_url, args.json_file)
    else:
        # If neither provided, use a default JSON file
        DEFAULT_JSON_PATH: str = os.path.join(os.getcwd(), "data", "raw", JSON_DATA_FILE_NAME)
        main_pipeline(db_url, DEFAULT_JSON_PATH)
