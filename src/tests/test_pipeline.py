import os
import json
import csv
import tempfile
import unittest
import datetime

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Import the pipeline functions and global variables from your module.
# Adjust the import path as needed.
from src.pipeline.unified_pipeline import (
    parse_json_to_csv,
    load_csv_to_stg,
    db_side_dedup,
    upsert_dimensions,
    insert_fact_listen,
    epoch_to_timestamp_str,
    CHECKPOINT_FILE,
    SCHEMA_SQL_PATH,
    CSV_TEMP_FILE,
    main_pipeline,
)

# Minimal schema SQL required for testing.
MINIMAL_SCHEMA_SQL = """
DROP TABLE IF EXISTS dim_user;
DROP TABLE IF EXISTS dim_track;
DROP TABLE IF EXISTS fact_listen;
DROP TABLE IF EXISTS stg_listens;
DROP TABLE IF EXISTS stg_listens_dedup;

CREATE TABLE dim_user (
    user_sk INTEGER,
    user_id VARCHAR
);

CREATE TABLE dim_track (
    track_sk INTEGER,
    track_id VARCHAR,
    track_name VARCHAR,
    artist_name VARCHAR
);

CREATE TABLE fact_listen (
    listen_sk INTEGER,
    user_sk INTEGER,
    track_sk INTEGER,
    listened_at TIMESTAMP
);
"""


class TestPipelineFunctions(unittest.TestCase):
    def setUp(self) -> None:
        # Create a temporary directory for test files.
        self.temp_dir = tempfile.TemporaryDirectory()
        # Override globals to use temporary files.
        global CHECKPOINT_FILE, SCHEMA_SQL_PATH, CSV_TEMP_FILE
        self.checkpoint_file = os.path.join(self.temp_dir.name, "checkpoint.json")
        self.schema_sql_file = os.path.join(self.temp_dir.name, "create_schema.sql")
        self.csv_temp_file = os.path.join(self.temp_dir.name, "temp_listens.csv")
        CHECKPOINT_FILE = self.checkpoint_file
        SCHEMA_SQL_PATH = self.schema_sql_file
        CSV_TEMP_FILE = self.csv_temp_file

        # Write the minimal schema to the temporary schema file.
        with open(self.schema_sql_file, "w", encoding="utf-8") as f:
            f.write(MINIMAL_SCHEMA_SQL)

        # Create an in-memory DuckDB instance.
        self.db_url = "duckdb:///:memory:"
        self.engine: Engine = create_engine(self.db_url, future=True)
        # Create the schema on the in-memory DB.
        with self.engine.begin() as conn:
            conn.execute(text(MINIMAL_SCHEMA_SQL))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def create_temp_json_file(self, records, filename: str = "test_data.json") -> str:
        """
        Write newline-delimited JSON records to a temporary file and return its path.
        """
        temp_json_file = os.path.join(self.temp_dir.name, filename)
        with open(temp_json_file, "w", encoding="utf-8") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")
        return temp_json_file

    def query_table_count(self, table_name: str) -> int:
        """
        Return the row count of the specified table.
        """
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.scalar()
        return count

    def test_parse_json_to_csv(self):
        """
        Test that parse_json_to_csv creates a CSV file with the expected rows and header.
        """
        records = [
            {
                "user_name": "user1",
                "recording_msid": "msid1",
                "listened_at": 1609459200,  # Jan 1, 2021 00:00:00 UTC
                "track_metadata": {"track_name": "Song A", "artist_name": "Artist X"}
            },
            {
                "user_name": "user2",
                "recording_msid": "msid2",
                "listened_at": 1609459300,
                "track_metadata": {"track_name": "Song B", "artist_name": "Artist Y"}
            }
        ]
        temp_json = self.create_temp_json_file(records, filename="parse_test.json")
        temp_csv = os.path.join(self.temp_dir.name, "output.csv")
        parse_json_to_csv(temp_json, temp_csv)

        # Verify CSV file exists and has header + 2 data rows.
        with open(temp_csv, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            rows = list(reader)
        # One header row + 2 data rows
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0], ["user_id", "track_id", "track_name", "artist_name", "listened_at"])

    def test_deduplication_single_file(self):
        """
        Test deduplication for a single JSON file containing duplicate records.
        """
        # Create a JSON file with a duplicate record.
        records = [
            {
                "user_name": "user1",
                "recording_msid": "msid1",
                "listened_at": 1609459200,
                "track_metadata": {"track_name": "Song A", "artist_name": "Artist X"}
            },
            {
                "user_name": "user1",
                "recording_msid": "msid1",  # duplicate record
                "listened_at": 1609459200,
                "track_metadata": {"track_name": "Song A", "artist_name": "Artist X"}
            },
            {
                "user_name": "user2",
                "recording_msid": "msid2",
                "listened_at": 1609459300,
                "track_metadata": {"track_name": "Song B", "artist_name": "Artist Y"}
            }
        ]
        temp_json = self.create_temp_json_file(records, filename="dedup_single.json")
        # Run the functions individually.
        parse_json_to_csv(temp_json, CSV_TEMP_FILE)
        load_csv_to_stg(self.engine, CSV_TEMP_FILE, stg_table="stg_listens", chunksize=50000)
        db_side_dedup(self.engine, stg_table="stg_listens", stg_dedup="stg_listens_dedup")
        # Expecting 2 unique rows in the deduplicated staging table.
        dedup_count = self.query_table_count("stg_listens_dedup")
        self.assertEqual(dedup_count, 2)

    def test_deduplication_multiple_files(self):
        """
        Test that processing two JSON files (with some overlapping records) results
        in deduplicated data in the final fact_listen table.
        """
        # --- Process first file ---
        records1 = [
            {
                "user_name": "user1",
                "recording_msid": "msid1",
                "listened_at": 1609459200,
                "track_metadata": {"track_name": "Song A", "artist_name": "Artist X"}
            },
            {
                "user_name": "user2",
                "recording_msid": "msid2",
                "listened_at": 1609459300,
                "track_metadata": {"track_name": "Song B", "artist_name": "Artist Y"}
            }
        ]
        temp_json1 = self.create_temp_json_file(records1, filename="file1.json")
        # Run pipeline steps for file1.
        parse_json_to_csv(temp_json1, CSV_TEMP_FILE)
        load_csv_to_stg(self.engine, CSV_TEMP_FILE, stg_table="stg_listens", chunksize=50000)
        db_side_dedup(self.engine, stg_table="stg_listens", stg_dedup="stg_listens_dedup")
        upsert_dimensions(self.engine)
        insert_fact_listen(self.engine)
        count_after_first = self.query_table_count("fact_listen")
        self.assertEqual(count_after_first, 2)

        # --- Process second file ---
        # Second file has one record that duplicates file1 and one new record.
        records2 = [
            {
                "user_name": "user1",
                "recording_msid": "msid1",  # overlapping duplicate
                "listened_at": 1609459200,
                "track_metadata": {"track_name": "Song A", "artist_name": "Artist X"}
            },
            {
                "user_name": "user3",
                "recording_msid": "msid3",
                "listened_at": 1609459400,
                "track_metadata": {"track_name": "Song C", "artist_name": "Artist Z"}
            }
        ]
        temp_json2 = self.create_temp_json_file(records2, filename="file2.json")
        # Clear the checkpoint (if any) so functions run anew.
        if os.path.exists(self.checkpoint_file):
            os.remove(self.checkpoint_file)
        # Process file2.
        parse_json_to_csv(temp_json2, CSV_TEMP_FILE)
        load_csv_to_stg(self.engine, CSV_TEMP_FILE, stg_table="stg_listens", chunksize=50000)
        db_side_dedup(self.engine, stg_table="stg_listens", stg_dedup="stg_listens_dedup")
        upsert_dimensions(self.engine)
        insert_fact_listen(self.engine)
        # Now, fact_listen should have 3 unique records (the overlapping record should not be inserted twice).
        count_after_second = self.query_table_count("fact_listen")
        self.assertEqual(count_after_second, 3)


if __name__ == '__main__':
    unittest.main()
