"""Resolve which PMIDs is the newest or if none is newer."""

import os
import sys
from pathlib import Path

import duckdb
from dotenv import load_dotenv


def find_duplicate_pmids(conn: duckdb.DuckDBPyConnection) -> None:
    """Find duplicate PMIDs in the pubmed table."""
    print("Finding duplicate PMIDs...")
    result = conn.execute(
        """--sql
        SELECT pmid, COUNT(*) AS count
        FROM pubmed
        GROUP BY pmid
        HAVING COUNT(*) > 1
    """,
    ).fetchall()

    if result:
        print(f"Found {len(result)} duplicate PMIDs:")
        for row in result[:30]:
            print("---First 30 duplicate PMIDs---")
            print(f"PMID: {row[0]}, Count: {row[1]}")
    else:
        print("No duplicate PMIDs found.")


def resolve_duplicate_pmids(conn: duckdb.DuckDBPyConnection) -> None:
    """Resolve duplicate PMIDs by keeping the newest entry.

    Simple algorithm
    1. Find duplicate PMIDs.
    2. For each duplicate, mark the older ones with is_duplicate = TRUE
       and the newest one with is_duplicate = FALSE
    """
    print("Resolving duplicate PMIDs...")

    # First, add the is_duplicate column if it doesn't exist
    conn.execute(
        """--sql
        ALTER TABLE pubmed ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN DEFAULT FALSE;
        """,
    )

    # Reset is_duplicate to FALSE for all entries for reruns
    conn.execute(
        """--sql
        UPDATE pubmed SET is_duplicate = FALSE;
        """,
    )

    # Mark the older duplicate entries as duplicates
    conn.execute(
        """--sql
        UPDATE pubmed
        SET is_duplicate = TRUE
        WHERE (pmid, date_resolved) IN (
            SELECT pmid, date_resolved
            FROM pubmed p1
            WHERE EXISTS (
                SELECT 1
                FROM pubmed p2
                WHERE p1.pmid = p2.pmid
                AND p2.date_resolved < p1.date_resolved -- Keep the newest
            )
        );
        """,
    )

    # Count how many entries were marked as duplicates
    result = conn.execute(
        """--sql
        SELECT COUNT(*) FROM pubmed WHERE is_duplicate = TRUE;
        """,
    ).fetchone()

    duplicate_count = result[0] if result else 0
    print(f"Marked {duplicate_count} entries as duplicates.")

    print("Duplicate PMIDs resolved.")


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
        find_duplicate_pmids(conn)
        print("---------------------------------------------------")
        resolve_duplicate_pmids(conn)
    except KeyboardInterrupt:
        print("KeyboardInterrupt: Exiting the script.")
        sys.exit(0)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
