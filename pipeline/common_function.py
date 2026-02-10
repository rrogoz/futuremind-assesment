import json
import os
import sys
from pathlib import Path
import pandas as pd
import json
from pathlib import Path
from datetime import datetime

import time
from datetime import datetime
from pathlib import Path
import json

def readConfig(pipeline_id):
    """
    Read pipeline configuration from JSON file.

    Args:
        pipeline_name (str): Name of the pipeline (config filename without .json)

    Returns:
        dict: Configuration dictionary

    Raises:
        SystemExit: If config file not found or invalid JSON
    """
    # Construct path relative to pipeline/ directory
    config_path = Path(f"../metadata/config/{pipeline_id}.json")

    # Check if file exists
    if not config_path.exists():
        print(f"⚠️  WARNING: Config file not found at {config_path.resolve()}")
        print(f"Expected location: metadata/config/{pipeline_id}.json")
        sys.exit(1)

    # Try to read and parse JSON
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        print(f"✓ Config loaded successfully: {pipeline_id}.json")
        return config

    except json.JSONDecodeError as e:
        print(f"⚠️  WARNING: Invalid JSON in {pipeline_id}")
        print(f"Error: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"⚠️  WARNING: Error reading config file")
        print(f"Error: {e}")
        sys.exit(1)


def appendMode(df: pd.DataFrame, path: str, format: str = "parquet", partition_cols: list = []):
    """
    Append dataframe to existing file or create new one.

    Args:
        df: pandas DataFrame to write
        path: file path (e.g., "data/bronze/revenues/data.parquet")
        format: file format ("parquet", "csv", "json")
    """
    from pathlib import Path

    # Create directory if doesn"t exist
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    # Check if file exists
    if Path(path).exists():
        # Read existing data
        if format == "parquet":
            existing_df = pd.read_parquet(path)
        elif format == "csv":
            existing_df = pd.read_csv(path)
        elif format == "json":
            existing_df = pd.read_json(path)
        else:
            raise ValueError(f"Unsupported format: {format}")

        # Append new data
        combined_df = pd.concat([existing_df, df], ignore_index=True)

        # Write combined data
        if format == "parquet":
            combined_df.to_parquet(path, index=False, partition_cols= partition_cols)
        elif format == "csv":
            combined_df.to_csv(path, index=False)
        elif format == "json":
            combined_df.to_json(path, orient="records", lines=True)

        print(
            f"✓ Appended {len(df)} records to {path} (total: {len(combined_df)})")
    else:
        # Create new file
        if format == "parquet":
            df.to_parquet(path, index=False, partition_cols= partition_cols)
        elif format == "csv":
            df.to_csv(path, index=False)
        elif format == "json":
            df.to_json(path, orient="records", lines=True)

        print(f"✓ Created {path} with {len(df)} records")


def absPath():
    return r"C:\Users\rogoz\Documents\own_projects\futuremind-assesment"



def updatePipelineStatus(pipeline_id: str, status: str = 'success'):
    """
    Write pipeline execution status to metadata file.
    
    Args:
        pipeline_id: Pipeline identifier (e.g., 'Revenues-Bronze')
        status: Execution status ('success', 'failed', etc.)
    """
    # Prepare metadata path
    metadata_dir = Path("../metadata/status/")
    metadata_dir.mkdir(parents=True, exist_ok=True)
    metadata_file = metadata_dir / f"{pipeline_id}.json"
    
    # Current timestamps
    current_time = datetime.now()
    current_unix = int(time.time())
    
    # Read existing metadata if exists
    if metadata_file.exists():
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
    else:
        metadata = {
            "pipeline_id": pipeline_id,
            "created_at": current_time.isoformat(),
            "created_at_unix": current_unix
        }
    
    # Update with current execution info
    metadata["last_run_status"] = status
    metadata["last_run_timestamp"] = current_time.isoformat()
    metadata["last_run_timestamp_unix"] = current_unix
    metadata["last_run_date"] = current_time.strftime('%Y-%m-%d')
    
    if status == 'success':
        metadata["last_success_timestamp"] = current_time.isoformat()
        metadata["last_success_timestamp_unix"] = current_unix
        metadata["last_success_date"] = current_time.strftime('%Y-%m-%d')
    
    # Write updated metadata
    with open(metadata_file, 'w') as f:
        json.dump(metadata, indent=2, fp=f)
    
    print(f"✓ Updated pipeline status: {pipeline_id} - {status}")


def getLastSuccessUnix(pipeline_id: str) -> int:
    """
    Read last successful execution Unix timestamp from metadata.
    
    Args:
        pipeline_id: Pipeline identifier (e.g., 'Revenues-Bronze')
        
    Returns:
        int: Unix timestamp of last successful run, or 0 if not found
    """
    from pathlib import Path
    import json
    
    metadata_file = Path(f"../metadata/status/{pipeline_id}.json")
    
    # Check if metadata file exists
    if not metadata_file.exists():
        return 0
    
    # Read metadata
    try:
        with open(metadata_file, 'r') as f:
            metadata = json.load(f)
        
        # Get last success Unix timestamp, default to 0
        return metadata.get('last_success_timestamp_unix', 0)
            
    except:
        return 0


def loadBronzeInDelta(bronze_path: str, partition_col: str, last_success_unix: int) -> pd.DataFrame:
    """
    Load Bronze data incrementally based on last success timestamp.
    
    Args:
        bronze_path: Path to bronze parquet data
        partition_col: Partition column name (e.g., '_tf_ingestion_time')
        last_success_unix: Last successful run Unix timestamp (0 for full load)
        
    Returns:
        pd.DataFrame: Filtered dataframe
    """
    
    df = pd.read_parquet(
        bronze_path,
        engine='fastparquet',
        filters=[(partition_col, '>', last_success_unix)]
    )
    
    print(f"✓ Loaded {len(df)} records from {bronze_path}")
    return df


def deduplicateRecords(df: pd.DataFrame, business_keys: list, order_by: list, ascending: bool = False) -> pd.DataFrame:
    """
    Deduplicate records based on business keys, keeping record with highest/lowest order_by values.
    
    Args:
        df: Input DataFrame
        business_keys: List of columns defining unique business records (e.g., ['id', 'date'])
        order_by: List of columns to order by before deduplication (e.g., ['_tf_ingestion_time'])
        ascending: If True, keep first (lowest); if False, keep last (highest)
        
    Returns:
        pd.DataFrame: Deduplicated DataFrame
    """
    
    initial_count = len(df)
    
    # Sort by order_by columns (descending by default to keep latest)
    df_sorted = df.sort_values(by=order_by, ascending=ascending)
    
    # Drop duplicates based on business keys, keeping first after sort
    df_deduped = df_sorted.drop_duplicates(subset=business_keys, keep='first')
    
    # Reset index
    df_deduped = df_deduped.reset_index(drop=True)
    
    duplicates_removed = initial_count - len(df_deduped)
    print(f"✓ Deduplication: {initial_count} → {len(df_deduped)} records ({duplicates_removed} duplicates removed)")
    
    return df_deduped


def mergeSilver(df_bronze: pd.DataFrame, target_path: str, primary_keys: list, order_by: list):
    """
    Simulate SQL MERGE statement with proper ordering.
    
    Args:
        df_bronze: New data from Bronze layer
        target_path: Path to Silver parquet file
        primary_keys: List of columns defining primary key (e.g., ['id', 'date'])
        order_by: List of columns to order by (e.g., ['_tf_ingestion_time', 'revenue'])
    """
    import pandas as pd
    from pathlib import Path
    
    Path(target_path).parent.mkdir(parents=True, exist_ok=True)
    
    # Read existing Silver
    try:
        df_silver = pd.read_parquet(target_path, engine='fastparquet')
    except:
        df_silver = pd.DataFrame()
    
    if df_silver.empty:
        df_bronze.to_parquet(target_path, index=False)
        print(f"✓ First load: {len(df_bronze)} records")
        return
    
    initial_silver_count = len(df_silver)
    
    # Concat both
    df_combined = pd.concat([df_silver, df_bronze], ignore_index=True)
    
    # Sort by order_by columns (ascending) so latest is last
    df_sorted = df_combined.sort_values(by=order_by, ascending=True)
    
    # Keep last (latest) for each primary key
    df_merged = df_sorted.drop_duplicates(subset=primary_keys, keep='last')
    
    # Write back
    df_merged.to_parquet(target_path, index=False)
    
    updates = len(df_bronze) - (len(df_merged) - initial_silver_count)
    inserts = len(df_merged) - initial_silver_count
    
    print(f"✓ MERGE complete:")
    print(f"  - Updated: {updates} records")
    print(f"  - Inserted: {inserts} records")
    print(f"  - Total: {len(df_merged)} records")


def createHashKey(df: pd.DataFrame, key_columns: list, hash_column: str = 'hash_key') -> pd.DataFrame:
    """
    Create a hash key from list of columns.
    
    Args:
        df: Input DataFrame
        key_columns: List of column names to hash
        hash_column: Name of the new hash column (default: 'hash_key')
        
    Returns:
        pd.DataFrame: DataFrame with new hash column
    """
    import hashlib
    import pandas as pd
    
    # Concatenate all key columns with a delimiter
    df[hash_column] = df[key_columns].astype(str).agg('|'.join, axis=1)
    
    # Apply MD5 hash
    df[hash_column] = df[hash_column].apply(
        lambda x: hashlib.md5(x.encode()).hexdigest()
    )
    
    return df


