"""
Example demonstrating the use of ReaderWriterPair with formatted table logging.

This script shows how to use the ReaderWriterPair class with the integrated
table formatting functionality to make log output more readable.
"""

import os
import sqlite3
import logging
from typing import List, Dict, Any

from scripts.sqlite_backend.core.db_engine import ReaderWriterPair
from scripts.sqlite_backend.db_main import EasyNerDBHandler


def example_process_function(batch: List[Dict], conn_params: Dict) -> List[Dict]:
    """
    Example process function that converts batch rows into a format suitable for writing.

    Args:
        batch: List of dictionaries with data to process
        conn_params: Connection parameters (unused in this example)

    Returns:
        Processed batch data
    """
    processed_data = []
    for row in batch:
        processed_data.append(
            {
                "id": row["id"],
                "text": row["text"].upper(),
                "processed": True,
                "length": len(row["text"]),
            }
        )
    return processed_data


def example_write_function(
    batch: List[Dict], cursor: sqlite3.Cursor, conn: sqlite3.Connection
) -> None:
    """
    Example write function that writes processed data to a database.

    Args:
        batch: List of dictionaries with processed data
        cursor: Database cursor for writing
        conn: Database connection
    """
    for item in batch:
        cursor.execute(
            "INSERT INTO processed_texts (text, is_processed, text_length) VALUES (?, ?, ?)",
            (item["text"], item["processed"], item["length"]),
        )


def setup_example_database(db_path: str) -> None:
    """
    Set up an example database for demonstration.

    Args:
        db_path: Path to the database file
    """
    # Create database and tables
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create source table
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS source_texts (
        id INTEGER PRIMARY KEY,
        text TEXT NOT NULL
    )
    """
    )

    # Create processed table
    cursor.execute(
        """
    CREATE TABLE IF NOT EXISTS processed_texts (
        id INTEGER PRIMARY KEY,
        text TEXT NOT NULL,
        is_processed BOOLEAN NOT NULL,
        text_length INTEGER NOT NULL
    )
    """
    )

    # Insert sample data
    cursor.execute("DELETE FROM source_texts")
    sample_texts = [
        "This is an example text.",
        "Another example with different content.",
        "Processing multiple texts in parallel.",
        "Using ReaderWriterPair for efficient database operations.",
        "Table formatting makes logs more readable.",
    ]

    for i, text in enumerate(sample_texts):
        cursor.execute(
            "INSERT INTO source_texts (id, text) VALUES (?, ?)", (i + 1, text)
        )

    conn.commit()
    conn.close()


def run_example() -> None:
    """Run the example to demonstrate ReaderWriterPair with table formatting."""
    # Create a temporary database
    db_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example.db")
    setup_example_database(db_path)

    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger("ReaderWriterExample")

    # Create connection parameters
    conn_params = {"database": db_path}

    # SQL query to read data
    query = """
    SELECT id, text FROM source_texts
    ORDER BY id
    LIMIT :limit OFFSET :offset
    """

    # Create and run ReaderWriterPair
    rwp = ReaderWriterPair(
        conn_params=conn_params,
        reader_query=query,
        process_function=example_process_function,
        write_function=example_write_function,
        total_rows=5,  # We know we have 5 sample texts
        logger=logger,
        batch_size=2,  # Small batch size for demonstration
        num_reader_threads=2,  # Use 2 reader threads
        max_queue_size=5,  # Small queue size for demonstration
        writer_batch_chunking=1,  # No batch chunking for this simple example
        process_title="Example Text Processing",
    )

    # Run the process
    rwp.run()

    # Verify results
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        "SELECT text, is_processed, text_length FROM processed_texts ORDER BY id"
    )
    results = cursor.fetchall()

    print("\nProcessed Results:")
    print("=" * 60)
    for i, result in enumerate(results):
        print(f"{i+1}. Text: {result[0]}, Processed: {result[1]}, Length: {result[2]}")
    print("=" * 60)

    conn.close()

    # Cleanup
    try:
        os.remove(db_path)
        print(f"Cleaned up example database: {db_path}")
    except OSError:
        print(f"Could not remove example database: {db_path}")


if __name__ == "__main__":
    run_example()
