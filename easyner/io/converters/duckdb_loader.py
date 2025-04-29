import duckdb
import json
import os
import glob
import sys
from tqdm import tqdm
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
import tempfile
import shutil
import warnings
import time
import numpy as np
from functools import lru_cache
from pathlib import Path
import logging

# Limit OpenBLAS threads to avoid resource exhaustion
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"

# Constants for configuration
DEFAULT_DB_PATH = "easyner.duckdb"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_JSON_DIR = (
    "/lunarc/nobackup/projects/snic2020-6-41/carl/data/ner_output"
)
BATCH_INSERT_SIZE = 1000
MAX_CONCURRENT_WORKERS = 32

# Database schema definitions
DB_SCHEMA = {
    "articles": """
        CREATE TABLE IF NOT EXISTS articles (
            article_id VARCHAR PRIMARY KEY,
            title TEXT NOT NULL
        )
    """,
    "sentences": """
        CREATE TABLE IF NOT EXISTS sentences (
            sentence_id INTEGER PRIMARY KEY,
            article_id VARCHAR NOT NULL,
            sentence_order INTEGER NOT NULL,
            text TEXT NOT NULL,
            FOREIGN KEY (article_id) REFERENCES articles(article_id)
        )
    """,
    "entity_occurrences": """
        CREATE TABLE IF NOT EXISTS entity_occurrences (
            occurrence_id INTEGER PRIMARY KEY,
            sentence_id INTEGER NOT NULL,
            entity_text VARCHAR NOT NULL,
            start_pos INTEGER NOT NULL,
            end_pos INTEGER NOT NULL,
            FOREIGN KEY (sentence_id) REFERENCES sentences(sentence_id)
        )
    """,
    "processing_progress": """
        CREATE TABLE IF NOT EXISTS processing_progress (
            file_path VARCHAR PRIMARY KEY,
            processed BOOLEAN,
            timestamp TIMESTAMP
        )
    """,
}

# SQL definitions for worker schema (without foreign keys)
WORKER_SCHEMA = {
    "articles": """
        CREATE TABLE articles (
            article_id VARCHAR PRIMARY KEY,
            title TEXT NOT NULL
        )
    """,
    "sentences": """
        CREATE TABLE sentences (
            sentence_id INTEGER PRIMARY KEY,
            article_id VARCHAR NOT NULL,
            sentence_order INTEGER NOT NULL,
            text TEXT NOT NULL
        )
    """,
    "entity_occurrences": """
        CREATE TABLE entity_occurrences (
            occurrence_id INTEGER PRIMARY KEY,
            sentence_id INTEGER NOT NULL,
            entity_text VARCHAR NOT NULL,
            start_pos INTEGER NOT NULL,
            end_pos INTEGER NOT NULL
        )
    """,
    "processing_progress": """
        CREATE TABLE processing_progress (
            file_path VARCHAR PRIMARY KEY,
            processed BOOLEAN,
            timestamp TIMESTAMP
        )
    """,
}

INDEX_DEFINITIONS = [
    "CREATE INDEX IF NOT EXISTS idx_sentences_article_id ON sentences(article_id)",
    "CREATE INDEX IF NOT EXISTS idx_entity_occurrences_sentence_id ON entity_occurrences(sentence_id)",
    "CREATE INDEX IF NOT EXISTS idx_entity_occurrences_entity_text ON entity_occurrences(entity_text)",
]

# SQL statements used for data insertion
SQL_STATEMENTS = {
    "article_insert": "INSERT OR IGNORE INTO articles (article_id, title) VALUES (?, ?)",
    "sentence_insert": "INSERT INTO sentences (sentence_id, article_id, sentence_order, text) VALUES (?, ?, ?, ?)",
    "entity_insert": "INSERT INTO entity_occurrences (occurrence_id, sentence_id, entity_text, start_pos, end_pos) VALUES (?, ?, ?, ?, ?)",
}

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("duckdb_loader")


def create_schema(db_path):
    """Create the database schema for the PubMed articles in a persistent DB file"""
    conn = duckdb.connect(db_path)

    # Create all tables from schema definitions
    for table_sql in DB_SCHEMA.values():
        conn.execute(table_sql)

    # Create all indexes
    for index_sql in INDEX_DEFINITIONS:
        conn.execute(index_sql)

    return conn


def setup_worker_database(worker_db_path):
    """Create schema in a worker database"""
    # Remove existing DB if present to ensure fresh start
    if os.path.exists(worker_db_path):
        os.remove(worker_db_path)

    conn = duckdb.connect(worker_db_path)

    # Create worker tables (without foreign keys for simplicity)
    for table_sql in WORKER_SCHEMA.values():
        conn.execute(table_sql)

    # Create indexes
    for index_sql in INDEX_DEFINITIONS:
        conn.execute(index_sql)

    return conn


def get_unprocessed_files(db_path, json_dir):
    """Get list of files that haven't been processed yet"""
    # Get list of all JSON files
    json_files = sorted(glob.glob(os.path.join(json_dir, "*.json")))
    total_files = len(json_files)

    if total_files == 0:
        logger.info(f"No JSON files found in {json_dir}")
        return [], 0

    logger.info(f"Found {total_files} JSON files to check")

    # Get list of already processed files
    conn = duckdb.connect(db_path)
    conn.execute(DB_SCHEMA["processing_progress"])  # Ensure table exists

    processed_files = set(
        row[0]
        for row in conn.execute(
            "SELECT file_path FROM processing_progress WHERE processed = TRUE"
        ).fetchall()
    )
    conn.close()

    # Determine files that need processing
    files_to_process = [f for f in json_files if f not in processed_files]

    return files_to_process, total_files


def process_json_files(db_path, json_dir, batch_size=10, num_workers=None):
    """Process all JSON files in the directory with parallel processing"""
    files_to_process, total_files = get_unprocessed_files(db_path, json_dir)

    if not files_to_process:
        logger.info("All files have already been processed.")
        return

    logger.info(f"Processing {len(files_to_process)} unprocessed files")

    # Determine number of workers (default to CPU count)
    if num_workers is None:
        # Use a more conservative number of workers to avoid resource exhaustion
        num_workers = max(
            1, min(MAX_CONCURRENT_WORKERS, multiprocessing.cpu_count() // 2)
        )

    logger.info(f"Using {num_workers} worker processes")

    # Create worker DB directory if needed
    worker_db_dir = Path(os.path.dirname(db_path)) / "worker_dbs"
    worker_db_dir.mkdir(exist_ok=True)

    # Process files sequentially if only 1 worker or in smaller batches if more
    if num_workers == 1:
        # Process files one by one with a single process
        logger.info("Using single-process mode")
        worker_db_path = str(worker_db_dir / "worker_single.duckdb")
        process_file_chunk(worker_db_path, files_to_process)

        # Merge the single worker database
        merge_worker_databases(db_path, [worker_db_path])

        # Clean up worker DB
        if os.path.exists(worker_db_path):
            os.remove(worker_db_path)
    else:
        # Process in batches with multiple workers
        process_with_multiple_workers(
            db_path, worker_db_dir, files_to_process, num_workers
        )

    logger.info("Processing complete!")


def process_with_multiple_workers(
    db_path, worker_db_dir, files_to_process, num_workers
):
    """Process files with multiple worker processes in batches"""
    # Use smaller chunks for better distribution
    chunk_size = max(1, min(3, len(files_to_process) // num_workers))
    file_chunks = [
        files_to_process[i : i + chunk_size]
        for i in range(0, len(files_to_process), chunk_size)
    ]

    # Process chunks sequentially in smaller batches to manage resources
    max_concurrent = min(MAX_CONCURRENT_WORKERS, num_workers)
    batch_size = max(
        1, len(file_chunks) // 5
    )  # Process ~20% of chunks at a time
    total_batches = (len(file_chunks) + batch_size - 1) // batch_size

    for batch_idx in range(0, len(file_chunks), batch_size):
        batch_end = min(batch_idx + batch_size, len(file_chunks))
        current_batch = file_chunks[batch_idx:batch_end]
        current_batch_num = batch_idx // batch_size + 1

        logger.info(
            f"Processing batch {current_batch_num}/{total_batches} "
            f"({batch_idx}-{batch_end-1} of {len(file_chunks)} chunks)"
        )

        # Process batch with ProcessPoolExecutor
        worker_dbs = process_batch(
            worker_db_dir, batch_idx, current_batch, max_concurrent
        )

        # Merge results after each batch
        if worker_dbs:
            logger.info(
                f"Merging batch {current_batch_num} worker databases..."
            )
            merge_worker_databases(db_path, worker_dbs)

            # Clean up worker DBs after merging
            clean_worker_databases(worker_dbs)

        # Pause between batches to let system recover
        time.sleep(1)


def clean_worker_databases(worker_dbs):
    """Clean up worker databases"""
    for worker_db in worker_dbs:
        if os.path.exists(worker_db):
            try:
                os.remove(worker_db)
            except OSError:
                logger.warning(f"Could not remove worker database {worker_db}")


def process_batch(worker_db_dir, batch_idx, current_batch, max_concurrent):
    """Process a batch of file chunks with ProcessPoolExecutor"""
    worker_dbs = []

    with ProcessPoolExecutor(max_workers=max_concurrent) as executor:
        futures = []
        for i, file_chunk in enumerate(current_batch):
            worker_db_path = str(
                Path(worker_db_dir) / f"worker_{batch_idx+i}.duckdb"
            )
            worker_dbs.append(worker_db_path)

            futures.append(
                executor.submit(process_file_chunk, worker_db_path, file_chunk)
            )

        # Process results as they complete
        for i, future in enumerate(futures):
            try:
                processed_count = future.result()
                logger.info(
                    f"Worker {batch_idx+i} completed processing {processed_count} files"
                )
            except Exception as e:
                logger.error(f"Worker {batch_idx+i} failed: {str(e)}")

    # Only return paths to existing worker databases
    return [path for path in worker_dbs if os.path.exists(path)]


def process_file_chunk(worker_db_path, file_chunk):
    """Process a chunk of files in a separate worker process"""
    try:
        # Create and set up the worker database
        conn = setup_worker_database(worker_db_path)

        # Start with sentence_id = 1 and occurrence_id = 1 for each worker
        sentence_id_start = 0
        occurrence_id_start = 0

        # Process each file in the chunk
        processed_count = 0
        for file_path in file_chunk:
            # Process the file
            result = process_single_file(
                conn, file_path, sentence_id_start, occurrence_id_start
            )
            if result:
                sentence_id_start, occurrence_id_start = result

                # Mark file as processed
                conn.execute(
                    "INSERT INTO processing_progress VALUES (?, TRUE, CURRENT_TIMESTAMP)",
                    [file_path],
                )
                processed_count += 1

        conn.commit()
        conn.close()
        return processed_count

    except Exception as e:
        logger.error(f"Error in worker: {str(e)}")
        return 0


def process_single_file(
    conn, json_file, sentence_id_start, occurrence_id_start
):
    """Process a single JSON file incrementally"""
    try:
        # Use a streaming JSON parser to avoid loading entire file into memory
        sentence_id = sentence_id_start + 1
        occurrence_id = occurrence_id_start + 1

        # Get SQL statements
        article_insert_sql = SQL_STATEMENTS["article_insert"]
        sentence_insert_sql = SQL_STATEMENTS["sentence_insert"]
        entity_insert_sql = SQL_STATEMENTS["entity_insert"]

        # Process the file
        with open(json_file, "r") as f:
            data = json.load(f)

            # Batch process to reduce memory usage
            article_batch = []
            sentence_batch = []
            entity_batch = []

            # Process each article
            for article_id, article_data in data.items():
                # Check if article already exists - use LIMIT 1 for efficiency
                exists = (
                    conn.execute(
                        "SELECT 1 FROM articles WHERE article_id = ? LIMIT 1",
                        [article_id],
                    ).fetchone()
                    is not None
                )

                if not exists:
                    article_batch.append(
                        (article_id, article_data.get("title", ""))
                    )

                # Process each sentence
                sentences = article_data.get("sentences", [])
                for sent_idx, sentence in enumerate(sentences):
                    text = sentence.get("text", "")
                    sentence_batch.append(
                        (sentence_id, article_id, sent_idx, text)
                    )

                    # Get entities and spans together
                    entities = sentence.get("entities", [])
                    entity_spans = sentence.get("entity_spans", [])

                    # Process entities efficiently with zip
                    for entity_text, span in zip(entities, entity_spans):
                        start_pos, end_pos = span
                        entity_batch.append(
                            (
                                occurrence_id,
                                sentence_id,
                                entity_text,
                                start_pos,
                                end_pos,
                            )
                        )
                        occurrence_id += 1

                    sentence_id += 1

                    # Insert in batches to manage memory
                    if len(sentence_batch) >= BATCH_INSERT_SIZE:
                        # Insert accumulated batches
                        insert_batches(
                            conn,
                            article_batch,
                            sentence_batch,
                            entity_batch,
                            article_insert_sql,
                            sentence_insert_sql,
                            entity_insert_sql,
                        )

                        # Clear batches
                        article_batch = []
                        sentence_batch = []
                        entity_batch = []

            # Insert any remaining records
            insert_batches(
                conn,
                article_batch,
                sentence_batch,
                entity_batch,
                article_insert_sql,
                sentence_insert_sql,
                entity_insert_sql,
            )

        return sentence_id - 1, occurrence_id - 1

    except Exception as e:
        logger.error(f"Error processing file {json_file}: {str(e)}")
        return sentence_id_start, occurrence_id_start


def insert_batches(
    conn,
    article_batch,
    sentence_batch,
    entity_batch,
    article_sql,
    sentence_sql,
    entity_sql,
):
    """Helper function to insert batches of records"""
    # Avoid empty executemany calls
    if article_batch:
        conn.executemany(article_sql, article_batch)

    if sentence_batch:
        conn.executemany(sentence_sql, sentence_batch)

    if entity_batch:
        conn.executemany(entity_sql, entity_batch)


def merge_worker_databases(db_path, worker_dbs):
    """Merge worker databases into the main database"""
    if not worker_dbs:
        return

    main_conn = duckdb.connect(db_path)

    for i, worker_db_path in enumerate(worker_dbs):
        if not os.path.exists(worker_db_path):
            continue

        logger.info(
            f"Merging database {i+1}/{len(worker_dbs)}: {worker_db_path}"
        )

        try:
            # Attach worker database
            main_conn.execute(f"ATTACH '{worker_db_path}' AS worker")

            # Merge articles table
            main_conn.execute(
                """
                INSERT INTO articles
                SELECT * FROM worker.articles
                ON CONFLICT (article_id) DO NOTHING
            """
            )

            # Get current max IDs from main database
            max_sentence_id = main_conn.execute(
                "SELECT COALESCE(MAX(sentence_id), 0) FROM sentences"
            ).fetchone()[0]
            max_occurrence_id = main_conn.execute(
                "SELECT COALESCE(MAX(occurrence_id), 0) FROM entity_occurrences"
            ).fetchone()[0]

            # Create temporary mapping table for sentence_ids
            main_conn.execute(
                "CREATE TEMPORARY TABLE IF NOT EXISTS sentence_id_mapping (old_id INTEGER, new_id INTEGER)"
            )
            main_conn.execute("DELETE FROM sentence_id_mapping")

            # Map sentence IDs
            main_conn.execute(
                f"""
                INSERT INTO sentence_id_mapping
                SELECT sentence_id as old_id,
                       sentence_id + {max_sentence_id} as new_id
                FROM worker.sentences
            """
            )

            # Insert sentences with new IDs
            main_conn.execute(
                f"""
                INSERT INTO sentences
                SELECT m.new_id, s.article_id, s.sentence_order, s.text
                FROM worker.sentences s
                JOIN sentence_id_mapping m ON s.sentence_id = m.old_id
            """
            )

            # Insert entity occurrences with adjusted sentence_ids
            main_conn.execute(
                f"""
                INSERT INTO entity_occurrences
                SELECT
                    occurrence_id + {max_occurrence_id} as occurrence_id,
                    m.new_id as sentence_id,
                    entity_text,
                    start_pos,
                    end_pos
                FROM worker.entity_occurrences e
                JOIN sentence_id_mapping m ON e.sentence_id = m.old_id
            """
            )

            # Merge processing progress
            main_conn.execute(
                """
                INSERT INTO processing_progress
                SELECT * FROM worker.processing_progress
                ON CONFLICT (file_path) DO UPDATE SET
                    processed = excluded.processed,
                    timestamp = excluded.timestamp
            """
            )

            # Detach worker database
            main_conn.execute("DETACH worker")

        except Exception as e:
            logger.error(f"Error merging database {worker_db_path}: {str(e)}")
            # Ensure we detach even if there's an error
            try:
                main_conn.execute("DETACH IF EXISTS worker")
            except:
                pass

    # Drop temporary tables
    main_conn.execute("DROP TABLE IF EXISTS sentence_id_mapping")

    main_conn.commit()
    main_conn.close()


def demonstrate_queries(db_path):
    """Show some example queries using the loaded data"""
    conn = duckdb.connect(db_path)

    logger.info("Database statistics:")
    logger.info("-------------------")

    logger.info("Total number of articles:")
    logger.info(conn.execute("SELECT COUNT(*) FROM articles").fetchall())

    logger.info("\nTotal number of sentences:")
    logger.info(conn.execute("SELECT COUNT(*) FROM sentences").fetchall())

    logger.info("\nTotal number of entity occurrences:")
    logger.info(
        conn.execute("SELECT COUNT(*) FROM entity_occurrences").fetchall()
    )

    logger.info("\nMost common entities:")
    logger.info(
        conn.execute(
            """
    SELECT entity_text, COUNT(*) as occurrences
    FROM entity_occurrences
    GROUP BY entity_text
    ORDER BY occurrences DESC
    LIMIT 10
    """
        ).fetchall()
    )

    logger.info("\nSentences with most entities:")
    logger.info(
        conn.execute(
            """
    SELECT s.article_id, LEFT(s.text, 80) || '...' as text, COUNT(e.occurrence_id) as entity_count
    FROM sentences s
    JOIN entity_occurrences e ON s.sentence_id = e.sentence_id
    GROUP BY s.article_id, s.text
    ORDER BY entity_count DESC
    LIMIT 5
    """
        ).fetchall()
    )

    conn.close()


def get_file_size(file_path):
    """Get file size in bytes"""
    try:
        return os.path.getsize(file_path)
    except (OSError, FileNotFoundError):
        return 0


def export_to_parquet(db_path, output_dir):
    """Export tables to Parquet format for efficient storage and sharing"""
    output_dir = Path(output_dir)
    output_dir.mkdir(exist_ok=True)
    conn = duckdb.connect(db_path)

    logger.info(f"Exporting data to Parquet files in {output_dir}...")

    # Track original and compressed sizes
    original_size = get_file_size(db_path)
    parquet_total_size = 0

    # Create tracking table for export metrics
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS export_metrics (
        table_name VARCHAR PRIMARY KEY,
        db_size_bytes BIGINT,
        parquet_size_bytes BIGINT,
        compression_ratio DOUBLE
    )
    """
    )

    # Get tables info
    tables_info = conn.execute(
        """
        SELECT table_name, estimated_size
        FROM duckdb_tables()
        WHERE table_name IN ('articles', 'sentences', 'entity_occurrences')
    """
    ).fetchall()

    table_sizes = {table: size for table, size in tables_info}

    # Export articles table
    articles_path = str(output_dir / "articles.parquet")
    conn.execute(f"COPY articles TO '{articles_path}' (FORMAT PARQUET)")
    articles_size = get_file_size(articles_path)
    parquet_total_size += articles_size

    # Export sentences and entity_occurrences in chunks by article_id
    article_ids = conn.execute(
        "SELECT DISTINCT article_id FROM articles ORDER BY article_id"
    ).fetchall()
    article_ids = [row[0] for row in article_ids]

    sentence_size = entity_size = 0

    if article_ids:
        # Process in chunks
        chunk_results = export_chunks_to_parquet(conn, article_ids, output_dir)
        sentence_size = chunk_results["sentences"]
        entity_size = chunk_results["entities"]
    else:
        # Handle empty database case
        export_empty_tables(conn, output_dir)

    # Calculate total size
    parquet_total_size = articles_size + sentence_size + entity_size

    # Store metrics for all tables
    update_export_metrics(
        conn,
        table_sizes,
        {
            "articles": articles_size,
            "sentences": sentence_size,
            "entity_occurrences": entity_size,
        },
    )

    # Display compression metrics
    display_compression_metrics(conn, original_size, parquet_total_size)

    conn.close()


def export_chunks_to_parquet(conn, article_ids, output_dir):
    """Export sentences and entities in chunks based on article IDs"""
    chunk_size = max(1, len(article_ids) // 10)  # Split into ~10 chunks
    sentence_total_size = entity_total_size = 0

    for i in range(0, len(article_ids), chunk_size):
        chunk = article_ids[i : i + chunk_size]
        chunk_min = chunk[0]
        chunk_max = chunk[-1]
        chunk_num = i // chunk_size

        # Export sentences for this article_id range
        sentences_path = str(output_dir / f"sentences_{chunk_num}.parquet")
        conn.execute(
            f"""
        COPY (SELECT * FROM sentences WHERE article_id >= '{chunk_min}' AND article_id <= '{chunk_max}')
        TO '{sentences_path}' (FORMAT PARQUET)
        """
        )
        sentences_size = get_file_size(sentences_path)
        sentence_total_size += sentences_size

        # Export entity occurrences for sentences in this range
        entities_path = str(
            output_dir / f"entity_occurrences_{chunk_num}.parquet"
        )
        conn.execute(
            f"""
        COPY (
            SELECT e.*
            FROM entity_occurrences e
            JOIN sentences s ON e.sentence_id = s.sentence_id
            WHERE s.article_id >= '{chunk_min}' AND s.article_id <= '{chunk_max}'
        )
        TO '{entities_path}' (FORMAT PARQUET)
        """
        )
        entity_size = get_file_size(entities_path)
        entity_total_size += entity_size

    return {"sentences": sentence_total_size, "entities": entity_total_size}


def export_empty_tables(conn, output_dir):
    """Export empty tables when no data is present"""
    sentences_path = str(output_dir / "sentences_empty.parquet")
    conn.execute(
        f"COPY (SELECT * FROM sentences WHERE 1=0) TO '{sentences_path}' (FORMAT PARQUET)"
    )

    entities_path = str(output_dir / "entity_occurrences_empty.parquet")
    conn.execute(
        f"COPY (SELECT * FROM entity_occurrences WHERE 1=0) TO '{entities_path}' (FORMAT PARQUET)"
    )


def update_export_metrics(conn, table_sizes, parquet_sizes):
    """Update export metrics table with size information"""
    for table, db_size in table_sizes.items():
        parquet_size = parquet_sizes.get(table, 0)
        ratio = db_size / parquet_size if parquet_size > 0 else 0

        conn.execute(
            """
            INSERT OR REPLACE INTO export_metrics VALUES (?, ?, ?, ?)
        """,
            [table, db_size, parquet_size, ratio],
        )


def display_compression_metrics(conn, original_size, parquet_total_size):
    """Display metrics about the compression achieved"""
    logger.info("\nCompression Metrics:")
    logger.info("-------------------")
    logger.info(f"Original DuckDB size: {original_size / (1024*1024):.2f} MB")
    logger.info(
        f"Total Parquet size: {parquet_total_size / (1024*1024):.2f} MB"
    )

    if parquet_total_size > 0:
        logger.info(
            f"Overall compression ratio: {original_size / parquet_total_size:.2f}x"
        )
    else:
        logger.info("Overall compression ratio: N/A (no data)")

    logger.info("\nTable-wise compression:")
    metrics = conn.execute(
        """
        SELECT table_name,
               db_size_bytes / (1024*1024.0) as db_size_mb,
               parquet_size_bytes / (1024*1024.0) as parquet_size_mb,
               compression_ratio
        FROM export_metrics
        ORDER BY db_size_bytes DESC
    """
    ).fetchall()

    for table, db_mb, parquet_mb, ratio in metrics:
        logger.info(
            f"  {table}: {db_mb:.2f} MB → {parquet_mb:.2f} MB (compression: {ratio:.2f}x)"
        )

    logger.info("Export complete!")


def main():
    """Main entry point for the script"""
    db_path = DEFAULT_DB_PATH
    json_dir = DEFAULT_JSON_DIR
    output_dir = DEFAULT_OUTPUT_DIR

    # Create schema if needed
    conn = create_schema(db_path)
    conn.close()

    # Process all JSON files with multiprocessing
    # Use fewer workers (8 max) for better resource management
    num_workers = max(
        1, min(MAX_CONCURRENT_WORKERS, multiprocessing.cpu_count() // 2)
    )
    process_json_files(
        db_path, json_dir, batch_size=5, num_workers=num_workers
    )

    # Show example queries
    demonstrate_queries(db_path)

    # Export to Parquet
    export_to_parquet(db_path, output_dir)

    logger.info("Processing complete!")


if __name__ == "__main__":
    main()
