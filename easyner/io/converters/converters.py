"""
Utilities for converting between different data formats (JSON, Parquet, etc.)
"""

import os
import json
import logging
import traceback
import time
from pathlib import Path
from typing import List, Dict, Optional, Any, Union, Tuple, Iterable
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd

import duckdb
from tqdm import tqdm

from ..config import (
    PARQUET_SETTINGS,
    MAX_CONCURRENT_WORKERS,
    DEFAULT_JSON_DIR,
    DEFAULT_PARQUET_DIR,
    DEFAULT_DB_PATH,
    get_parquet_path,
    PARQUET_CHUNK_SIZE,
    setup_thread_limits,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def json_to_df(json_file_path: str) -> pd.DataFrame:
    """
    Convert a JSON file to a pandas DataFrame.

    Args:
        json_file_path: Path to the JSON file

    Returns:
        DataFrame containing the JSON data
    """
    try:
        with open(json_file_path, "r") as f:
            data = json.load(f)
        return pd.json_normalize(data)
    except Exception as e:
        logger.error(f"Error converting {json_file_path} to DataFrame: {e}")
        raise


def df_to_parquet(
    df: pd.DataFrame,
    output_path: str,
    compression: str = PARQUET_SETTINGS["compression"],
    row_group_size: int = PARQUET_SETTINGS["row_group_size"],
) -> None:
    """
    Convert a DataFrame to a Parquet file.

    Args:
        df: DataFrame to convert
        output_path: Path where to save the Parquet file
        compression: Compression algorithm to use
        row_group_size: Size of row groups in the Parquet file
    """
    try:
        df.to_parquet(
            output_path, compression=compression, row_group_size=row_group_size
        )
    except Exception as e:
        logger.error(
            f"Error converting DataFrame to Parquet at {output_path}: {e}"
        )
        raise


def process_json_file(
    json_file_path: str,
    parquet_dir: Optional[str] = None,
    chunk_size: int = PARQUET_CHUNK_SIZE,
) -> Tuple[str, bool, Optional[str]]:
    """
    Process a single JSON file and convert it to Parquet.

    Args:
        json_file_path: Path to the JSON file
        parquet_dir: Directory where to save Parquet files
        chunk_size: Number of rows per chunk for processing large files

    Returns:
        Tuple of (file_path, success_flag, error_message)
    """
    setup_thread_limits()  # Limit thread usage for each worker

    output_path = get_parquet_path(json_file_path, parquet_dir)

    try:
        # Create directory if needed
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Convert JSON to DataFrame
        df = json_to_df(json_file_path)

        if len(df) > chunk_size:
            # Process in chunks if the dataframe is large
            for i, chunk_df in enumerate(
                pd.read_json(json_file_path, chunksize=chunk_size)
            ):
                chunk_path = str(output_path).replace(
                    ".parquet", f"_part{i}.parquet"
                )
                df_to_parquet(chunk_df, chunk_path)
        else:
            # Process as a single file
            df_to_parquet(df, str(output_path))

        return json_file_path, True, None

    except Exception as e:
        error_msg = f"Error: {str(e)}\n{traceback.format_exc()}"
        logger.error(f"Failed to process {json_file_path}: {error_msg}")
        return json_file_path, False, error_msg


def find_json_files(json_dir: str, recursive: bool = True) -> List[str]:
    """
    Find all JSON files in a directory.

    Args:
        json_dir: Directory to search for JSON files
        recursive: Whether to search recursively in subdirectories

    Returns:
        List of paths to JSON files
    """
    json_files = []
    search_path = Path(json_dir)

    if recursive:
        pattern = "**/*.json"
    else:
        pattern = "*.json"

    for file_path in search_path.glob(pattern):
        json_files.append(str(file_path))

    return json_files


def get_unprocessed_files(
    conn: duckdb.DuckDBPyConnection,
    file_paths: List[str],
    target_format: str = "parquet",
) -> List[str]:
    """
    Filter out files that have already been converted.

    Args:
        conn: DuckDB connection
        file_paths: List of file paths to check
        target_format: The conversion format to check for

    Returns:
        List of files that haven't been processed yet
    """
    if not file_paths:
        return []

    # Create placeholders for SQL IN clause
    placeholders = ", ".join(["?"] * len(file_paths))

    # Check which files are already processed
    query = f"""
    SELECT file_path
    FROM conversion_progress
    WHERE file_path IN ({placeholders})
    AND target_format = ?
    AND converted = TRUE
    """

    # Execute query with parameters
    params = file_paths + [target_format]
    processed_files = conn.execute(query, params).fetchnumpy()

    if (
        "file_path" in processed_files
        and len(processed_files["file_path"]) > 0
    ):
        processed_set = set(processed_files["file_path"])
        return [f for f in file_paths if f not in processed_set]
    else:
        return file_paths


def update_conversion_progress(
    conn: duckdb.DuckDBPyConnection,
    results: List[Tuple[str, bool, Optional[str]]],
    target_format: str = "parquet",
) -> None:
    """
    Update the conversion progress in the database.

    Args:
        conn: DuckDB connection
        results: List of (file_path, success_flag, error_message) tuples
        target_format: The format of the conversion
    """
    # Prepare the data for insertion
    current_time = time.strftime("%Y-%m-%d %H:%M:%S")

    # Insert or update conversion progress records
    for file_path, success, error in results:
        conn.execute(
            """
            INSERT OR REPLACE INTO conversion_progress
            (file_path, converted, target_format, timestamp, error)
            VALUES (?, ?, ?, ?, ?)
            """,
            (file_path, success, target_format, current_time, error),
        )

    conn.commit()


def convert_json_to_parquet(
    json_dir: str = DEFAULT_JSON_DIR,
    parquet_dir: str = DEFAULT_PARQUET_DIR,
    db_path: str = DEFAULT_DB_PATH,
    max_workers: int = MAX_CONCURRENT_WORKERS,
    recursive: bool = True,
    show_progress: bool = True,
) -> Dict[str, int]:
    """
    Convert JSON files to Parquet format using multiprocessing.

    Args:
        json_dir: Directory with JSON files
        parquet_dir: Directory where to save Parquet files
        db_path: Path to the DuckDB database
        max_workers: Maximum number of concurrent workers
        recursive: Whether to process subdirectories
        show_progress: Whether to show a progress bar

    Returns:
        Dictionary with conversion statistics
    """
    # Ensure output directory exists
    os.makedirs(parquet_dir, exist_ok=True)

    # Find all JSON files
    logger.info(f"Searching for JSON files in: {json_dir}")
    json_files = find_json_files(json_dir, recursive)

    if not json_files:
        logger.warning(f"No JSON files found in {json_dir}")
        return {"total": 0, "processed": 0, "failed": 0}

    logger.info(f"Found {len(json_files)} JSON files")

    # Connect to the database
    conn = duckdb.connect(db_path)

    # Ensure the conversion_progress table exists
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS conversion_progress (
        file_path VARCHAR PRIMARY KEY,
        converted BOOLEAN DEFAULT FALSE,
        target_format VARCHAR NOT NULL,
        timestamp TIMESTAMP,
        error TEXT
    )
    """
    )

    # Filter out already processed files
    unprocessed_files = get_unprocessed_files(conn, json_files)
    logger.info(f"Found {len(unprocessed_files)} unprocessed JSON files")

    if not unprocessed_files:
        logger.info("All files have been processed already")
        return {
            "total": len(json_files),
            "processed": len(json_files),
            "failed": 0,
        }

    # Process files using a ProcessPoolExecutor
    results = []
    successful = 0
    failed = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_json_file, file_path, parquet_dir
            ): file_path
            for file_path in unprocessed_files
        }

        # Use tqdm for progress tracking if requested
        iterator = as_completed(futures)
        if show_progress:
            iterator = tqdm(
                iterator, total=len(futures), desc="Converting JSON to Parquet"
            )

        for future in iterator:
            try:
                result = future.result()
                results.append(result)
                if result[1]:  # Success flag
                    successful += 1
                else:
                    failed += 1
            except Exception as e:
                file_path = futures[future]
                logger.error(f"Exception when processing {file_path}: {e}")
                results.append((file_path, False, str(e)))
                failed += 1

    # Update the database with the results
    update_conversion_progress(conn, results)

    # Close connection
    conn.close()

    # Return statistics
    stats = {
        "total": len(json_files),
        "processed": successful,
        "failed": failed,
        "skipped": len(json_files) - len(unprocessed_files),
    }

    logger.info(f"Conversion complete: {stats}")
    return stats
