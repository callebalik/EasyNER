"""
Unit tests for the converter utilities.
"""

import os
import tempfile
import json
import shutil
import pytest
import pandas as pd
import duckdb

from pathlib import Path
from easyner.io.converters.converters import (
    json_to_df,
    df_to_parquet,
    process_json_file,
    find_json_files,
    get_unprocessed_files,
    update_conversion_progress,
)


@pytest.fixture
def sample_json_data():
    """Create sample JSON data for testing."""
    return [
        {"id": 1, "name": "Item 1", "value": 10.5},
        {"id": 2, "name": "Item 2", "value": 20.7},
        {"id": 3, "name": "Item 3", "value": 30.2},
    ]


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture
def temp_json_file(temp_dir, sample_json_data):
    """Create a temporary JSON file with sample data."""
    file_path = os.path.join(temp_dir, "sample.json")
    with open(file_path, "w") as f:
        json.dump(sample_json_data, f)
    return file_path


@pytest.fixture
def temp_db_file(temp_dir):
    """Create a temporary DuckDB database."""
    db_path = os.path.join(temp_dir, "test.duckdb")
    conn = duckdb.connect(db_path)
    conn.execute(
        """
    CREATE TABLE conversion_progress (
        file_path VARCHAR PRIMARY KEY,
        converted BOOLEAN DEFAULT FALSE,
        target_format VARCHAR NOT NULL,
        timestamp TIMESTAMP,
        error TEXT
    )
    """
    )
    conn.close()
    return db_path


def test_json_to_df(temp_json_file, sample_json_data):
    """Test JSON to DataFrame conversion."""
    df = json_to_df(temp_json_file)

    assert len(df) == len(sample_json_data)
    assert set(df.columns) == {"id", "name", "value"}
    assert df["id"].tolist() == [1, 2, 3]


def test_df_to_parquet(temp_dir, sample_json_data):
    """Test DataFrame to Parquet conversion."""
    # Create a DataFrame
    df = pd.DataFrame(sample_json_data)

    # Set output path
    output_path = os.path.join(temp_dir, "output.parquet")

    # Convert to Parquet
    df_to_parquet(df, output_path)

    # Verify the file exists
    assert os.path.exists(output_path)

    # Read back the Parquet file
    df_read = pd.read_parquet(output_path)

    # Check if the data is preserved
    assert len(df_read) == len(df)
    assert set(df_read.columns) == set(df.columns)


def test_process_json_file(temp_json_file, temp_dir):
    """Test processing a JSON file to Parquet."""
    # Process the file
    file_path, success, error = process_json_file(temp_json_file, temp_dir)

    # Check the result
    assert file_path == temp_json_file
    assert success is True
    assert error is None

    # Check if the Parquet file was created
    parquet_path = os.path.join(temp_dir, "sample.parquet")
    assert os.path.exists(parquet_path)


def test_find_json_files(temp_dir):
    """Test finding JSON files in a directory."""
    # Create test files
    os.makedirs(os.path.join(temp_dir, "subdir"))

    files = [
        os.path.join(temp_dir, "file1.json"),
        os.path.join(temp_dir, "file2.json"),
        os.path.join(temp_dir, "subdir", "file3.json"),
    ]

    for file_path in files:
        with open(file_path, "w") as f:
            f.write("{}")

    # Also create a non-JSON file
    with open(os.path.join(temp_dir, "file.txt"), "w") as f:
        f.write("text")

    # Test recursive search
    found_files = find_json_files(temp_dir, recursive=True)
    assert len(found_files) == 3
    for file_path in files:
        assert file_path in found_files

    # Test non-recursive search
    found_files = find_json_files(temp_dir, recursive=False)
    assert len(found_files) == 2
    assert files[0] in found_files
    assert files[1] in found_files
    assert files[2] not in found_files


def test_get_unprocessed_files(temp_db_file):
    """Test filtering out processed files."""
    conn = duckdb.connect(temp_db_file)

    # Add some processed files to the database
    conn.execute(
        "INSERT INTO conversion_progress VALUES (?, ?, ?, ?, ?)",
        ("file1.json", True, "parquet", "2023-01-01 00:00:00", None),
    )
    conn.execute(
        "INSERT INTO conversion_progress VALUES (?, ?, ?, ?, ?)",
        ("file2.json", False, "parquet", "2023-01-01 00:00:00", "Error"),
    )

    # Test filtering
    all_files = ["file1.json", "file2.json", "file3.json"]
    unprocessed = get_unprocessed_files(conn, all_files)

    # file1.json is processed successfully, file2.json had an error (should be retried)
    assert len(unprocessed) == 2
    assert "file1.json" not in unprocessed
    assert "file2.json" in unprocessed
    assert "file3.json" in unprocessed

    conn.close()


def test_update_conversion_progress(temp_db_file):
    """Test updating conversion progress."""
    conn = duckdb.connect(temp_db_file)

    # Prepare test data
    results = [
        ("file1.json", True, None),
        ("file2.json", False, "Error occurred"),
    ]

    # Update progress
    update_conversion_progress(conn, results)

    # Verify the updates
    rows = conn.execute(
        "SELECT file_path, converted, error FROM conversion_progress"
    ).fetchall()
    assert len(rows) == 2

    for row in rows:
        if row[0] == "file1.json":
            assert row[1] is True
            assert row[2] is None
        elif row[0] == "file2.json":
            assert row[1] is False
            assert row[2] == "Error occurred"

    conn.close()
