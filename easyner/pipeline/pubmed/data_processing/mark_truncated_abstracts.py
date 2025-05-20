"""Module for marking and processing truncated abstracts in PubMed data.

This module provides functionality to identify and handle abstracts that are truncated,
specifically those marked with for example '(ABSTRACT TRUNCATED AT 250 WORDS)'.

I believe from the pubmed docs that they started truncating abstracts in the olden days
when uploading. Then the limit has changed over time and now they are not truncating...
anymore.

TODO: Check if truncation is surronded by newline.
"""

import os
import sys

import duckdb
from dotenv import load_dotenv


def find_truncated_abstracts(conn: duckdb.DuckDBPyConnection) -> None:
    """Find mark in abstract text and remove it, marking the abstract as truncated."""
    descriptive_pattern = "(ABSTRACT TRUNCATED AT <number> WORDS)"
    regex_pattern = r"\(ABSTRACT TRUNCATED AT \d+ WORDS\)"
    # Check for the pattern in all abstracts using regex
    result = conn.execute(
        """--sql
        SELECT COUNT(*)
        FROM pubmed
        WHERE regexp_matches(abstract, ?)
    """,
        (regex_pattern,),
    ).fetchone()
    count = result[0] if result is not None else 0
    print(
        f"Found {count} abstracts with the truncation pattern matching '{descriptive_pattern}'",  # noqa: E501
    )


def remove_truncation_mark_and_add_truncated(conn: duckdb.DuckDBPyConnection) -> None:
    """Remove truncation mark from abstracts and add a 'truncated' flag."""
    descriptive_pattern = "(ABSTRACT TRUNCATED AT <number> WORDS)"
    regex_pattern = r"\(ABSTRACT TRUNCATED AT \d+ WORDS\)"  # Regex for "digits"
    # Add a new column to mark truncated abstracts
    conn.execute(
        """--sql
        ALTER TABLE pubmed
        ADD COLUMN IF NOT EXISTS
        is_truncated BOOLEAN DEFAULT FALSE""",
    )
    conn.execute(
        """--sql
        UPDATE pubmed
        SET abstract = regexp_replace(abstract, ?, '', 'g'),
            is_truncated = TRUE
        WHERE regexp_matches(abstract, ?)
    """,
        (regex_pattern, regex_pattern),
    )
    print(
        f"Removed truncation mark and marked abstracts as truncated "
        f"for patterns matching '{descriptive_pattern}'",
    )


if __name__ == "__main__":
    try:
        load_dotenv()
        DB_PATH = os.getenv("DB_PATH")
        if DB_PATH is None or DB_PATH.strip() == "":
            msg = "DB_PATH environment variable is not set."
            raise ValueError(msg)
        else:
            print(f"Using database path: {DB_PATH}")

        conn = duckdb.connect(DB_PATH)

        find_truncated_abstracts(conn)
        remove_truncation_mark_and_add_truncated(conn)
    except KeyboardInterrupt:
        print("KeyboardInterrupt: Exiting the script.")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
