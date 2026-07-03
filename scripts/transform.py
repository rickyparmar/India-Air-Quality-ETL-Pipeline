#!/usr/bin/env python3
"""
ETL Pipeline - Transformation Script
Reads raw JSON responses, flattens records into a DataFrame, performs type casting,
handles missing data, creates composite keys, deduplicates, and exports a clean CSV.
"""

import os
import sys
import glob
import json
import datetime
import pandas as pd

DEFAULT_PROCESSED_DIR = "/opt/airflow/data/processed"


def transform_data(raw_data_dir: str, processed_base_dir: str = DEFAULT_PROCESSED_DIR) -> str:
    """
    Reads raw JSON files from raw_data_dir, transforms, cleans, and saves to a CSV file.
    
    Args:
        raw_data_dir (str): Directory containing raw JSON files for this run.
        processed_base_dir (str): Directory where clean CSV should be saved.
        
    Returns:
        str: Path of the generated CSV file.
    """
    print(f"Starting transformation for raw data directory: {raw_data_dir}")
    
    # 1. Read and aggregate all JSON page files
    json_pattern = os.path.join(raw_data_dir, "page_*.json")
    json_files = glob.glob(json_pattern)
    
    if not json_files:
        raise FileNotFoundError(f"No JSON raw files found in {raw_data_dir} matching page_*.json")
        
    all_records = []
    for filepath in sorted(json_files):
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                payload = json.load(f)
                records = payload.get("records", [])
                all_records.extend(records)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse JSON file {filepath}: {e}. Skipping.")
                
    total_raw_rows = len(all_records)
    print(f"Aggregated {total_raw_rows} raw records.")
    
    if total_raw_rows == 0:
        raise ValueError("No records found in raw files to transform.")
        
    # Load into DataFrame
    df = pd.DataFrame(all_records)
    
    # 2. Type Casting and Null Handling
    numeric_cols = ["latitude", "longitude", "min_value", "max_value", "avg_value"]
    
    print("\n--- Performing Type Casting & Null/Invalid Reporting ---")
    for col in numeric_cols:
        if col in df.columns:
            # Check how many are currently null/missing before conversion
            pre_nulls = df[col].isna().sum() or (df[col] == '').sum() or (df[col] == 'NA').sum()
            
            # Coerce non-numeric values to NaN and explicitly cast to float
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
            
            # Report newly introduced nulls (due to non-numeric types) + total nulls
            total_nulls = df[col].isna().sum()
            affected = total_nulls - pre_nulls
            
            print(f"Column '{col}': Total Null/NaN = {total_nulls} (New coerced NaNs = {affected} from invalid strings)")
        else:
            print(f"Warning: Column '{col}' missing from raw API schema!")
            df[col] = pd.NA
            
    # 3. Parsing Date / Timestamp
    print("\n--- Parsing Timestamp Column ---")
    if "last_update" in df.columns:
        # Save pre-conversion state
        invalid_dates_pre = df["last_update"].isna().sum()
        
        # Parse last_update string (format 'DD-MM-YYYY HH:MM:SS')
        df["last_update"] = pd.to_datetime(df["last_update"], format="%d-%m-%Y %H:%M:%S", errors='coerce')
        
        invalid_dates_post = df["last_update"].isna().sum()
        date_coerced = invalid_dates_post - invalid_dates_pre
        print(f"Column 'last_update': Total NaT (invalid timestamps) = {invalid_dates_post} (Coerced to NaT = {date_coerced})")
        
        # Drop rows where last_update is NaT (since they cannot participate in key generation or analytics)
        if invalid_dates_post > 0:
            original_len = len(df)
            df = df.dropna(subset=["last_update"])
            dropped = original_len - len(df)
            print(f"Dropped {dropped} rows with missing or invalid 'last_update' timestamps.")
    else:
        raise ValueError("Critical Column 'last_update' is missing from raw dataset.")
        
    # 4. Remove exact duplicates
    len_before_dup = len(df)
    df.drop_duplicates(inplace=True)
    exact_duplicates = len_before_dup - len(df)
    print(f"\nExact duplicate rows dropped: {exact_duplicates}")
    
    # 5. Create unique composite key and handle collisions
    print("\n--- Generating Composite Key ---")
    # Composite key columns: station + pollutant_id + last_update
    # We clean these fields of leading/trailing spaces first
    df["station"] = df["station"].astype(str).str.strip()
    df["pollutant_id"] = df["pollutant_id"].astype(str).str.strip()
    
    # Generate concatenated key: 'Station Name|Pollutant_ID|YYYY-MM-DD HH:MM:SS'
    df["composite_key"] = (
        df["station"] + "|" + 
        df["pollutant_id"] + "|" + 
        df["last_update"].dt.strftime("%Y-%m-%d %H:%M:%S")
    )
    
    # Verify unique composite keys (just in case multiple records have the same keys but different attributes)
    len_before_key_dup = len(df)
    df.drop_duplicates(subset=["composite_key"], keep="first", inplace=True)
    key_duplicates = len_before_key_dup - len(df)
    if key_duplicates > 0:
        print(f"Warning: Found {key_duplicates} records with matching composite key, but different row values. Kept first.")
        
    # 6. Final Clean-up and Export
    os.makedirs(processed_base_dir, exist_ok=True)
    
    # Extract directory name from raw_data_dir to name the processed file (maintains partition alignment)
    run_folder_name = os.path.basename(os.path.normpath(raw_data_dir))
    processed_filepath = os.path.join(processed_base_dir, f"{run_folder_name}_cleaned.csv")
    
    df.to_csv(processed_filepath, index=False, encoding="utf-8")
    print(f"\nTransformation completed. Transformed {len(df)} rows.")
    print(f"Processed file saved to: {processed_filepath}")
    
    return processed_filepath


if __name__ == "__main__":
    # If run standalone, require a directory argument
    if len(sys.argv) < 2:
        print("Usage: python transform.py <path_to_raw_run_directory>")
        sys.exit(1)
        
    raw_dir_input = sys.argv[1]
    
    # Resolve local directory paths if not in Docker container
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    is_docker = os.path.exists("/.dockerenv") or os.environ.get("AIRFLOW_HOME") is not None
    local_processed_dir = DEFAULT_PROCESSED_DIR if is_docker else os.path.join(project_root, "data", "processed")
    
    try:
        csv_file = transform_data(raw_data_dir=raw_dir_input, processed_base_dir=local_processed_dir)
        print(f"SUCCESS: Transformed data stored in {csv_file}")
    except Exception as exc:
        print(f"FAILURE during transformation: {exc}")
        sys.exit(1)
