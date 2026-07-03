#!/usr/bin/env python3
"""
ETL Pipeline - Load Script
Loads clean CSV air quality data into PostgreSQL using bulk upsert.
Reports exact counts of inserted vs. updated rows.
"""

import os
import sys
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Database connection credentials placeholder defaults
DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5432


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Establishes connection to the target PostgreSQL database using environment variables.
    """
    user = os.getenv("ANALYTICS_DB_USER")
    password = os.getenv("ANALYTICS_DB_PASSWORD")
    db_name = os.getenv("ANALYTICS_DB_NAME")
    host = os.getenv("ANALYTICS_DB_HOST", DEFAULT_HOST)
    port = os.getenv("ANALYTICS_DB_PORT", str(DEFAULT_PORT))
    
    if not all([user, password, db_name]):
        raise ValueError("Database credentials (user, password, db_name) must be set in environment variables.")
        
    print(f"Connecting to database '{db_name}' on {host}:{port} as user '{user}'...")
    return psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )


def load_data(csv_file_path: str) -> None:
    """
    Loads data from CSV file, performs bulk upsert, and logs metrics.
    
    Args:
        csv_file_path (str): Path to the cleaned CSV file.
    """
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"Cleaned CSV file not found at: {csv_file_path}")
        
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)
    num_input_rows = len(df)
    print(f"Loaded {num_input_rows} rows from clean CSV file.")
    
    if num_input_rows == 0:
        print("CSV is empty. Nothing to load.")
        return
        
    # Prepare DataFrame records for psycopg2: replace all NaN/NaT with None (mapped to SQL NULL)
    df_clean = df.where(pd.notnull(df), None)
    
    # Select columns in exact order corresponding to SQL statement parameters
    # The order must match:
    # composite_key, country, state, city, station, last_update, latitude, longitude, pollutant_id, min_value, max_value, avg_value
    records_to_insert = df_clean[[
        "composite_key", "country", "state", "city", "station", "last_update",
        "latitude", "longitude", "pollutant_id", "min_value", "max_value", "avg_value"
    ]].values.tolist()
    
    # Establish connection and perform upsert
    conn = get_db_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                # 1. Fetch current row count before upsert
                cursor.execute("SELECT COUNT(*) FROM air_quality_readings;")
                count_before = cursor.fetchone()[0]
                print(f"Table row count BEFORE load: {count_before}")
                
                # 2. Execute bulk upsert (INSERT ... ON CONFLICT DO UPDATE)
                upsert_query = """
                    INSERT INTO air_quality_readings (
                        composite_key, country, state, city, station, last_update,
                        latitude, longitude, pollutant_id, min_value, max_value, avg_value
                    ) VALUES %s
                    ON CONFLICT (composite_key) DO UPDATE SET
                        country = EXCLUDED.country,
                        state = EXCLUDED.state,
                        city = EXCLUDED.city,
                        station = EXCLUDED.station,
                        last_update = EXCLUDED.last_update,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        pollutant_id = EXCLUDED.pollutant_id,
                        min_value = EXCLUDED.min_value,
                        max_value = EXCLUDED.max_value,
                        avg_value = EXCLUDED.avg_value,
                        updated_at = NOW();
                """
                
                print("Executing bulk upsert query...")
                execute_values(cursor, upsert_query, records_to_insert)
                
                # 3. Fetch current row count after upsert
                cursor.execute("SELECT COUNT(*) FROM air_quality_readings;")
                count_after = cursor.fetchone()[0]
                print(f"Table row count AFTER load: {count_after}")
                
                # 4. Perform metrics logging logic
                # Since composite_keys are unique, the table growth represents new inserts.
                # Any other record in the CSV that did not increase the count must be an update.
                inserts = count_after - count_before
                updates = num_input_rows - inserts
                
                print("\n--- Load Metrics Summary ---")
                print(f"Total Rows Processed: {num_input_rows}")
                print(f"Rows Inserted (New):  {inserts}")
                print(f"Rows Updated (Exist): {updates}")
                print("Transaction committed successfully.")
                
    except Exception as e:
        print(f"Error executing database load transaction: {e}")
        conn.rollback()
        raise e
    finally:
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    # If run standalone, require a CSV filepath
    if len(sys.argv) < 2:
        print("Usage: python load.py <path_to_cleaned_csv>")
        sys.exit(1)
        
    csv_input = sys.argv[1]
    
    # Load environment variables for standalone script testing
    load_dotenv()
    
    try:
        load_data(csv_file_path=csv_input)
        print("SUCCESS: Data loading completed.")
    except Exception as exc:
        print(f"FAILURE during loading: {exc}")
        sys.exit(1)
