import json
import sqlite3
import os
import gc
import time
import logging
import psutil
from glob import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from typing import List, Tuple, Dict

from db_main import EasyNerDBHandler

def get_memory_usage():
    """Get current memory usage in MB"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def process_chunk(chunk):
    documents = []
    sentences = []
    named_entities = {"disease": 1, "phenomenon": 2}  # Fixed entity types
    entity_occurrences = []

    try:
        # Pre-allocate approximate size for lists based on typical ratios
        chunk_size = len(chunk)
        documents = [] if not chunk_size else [None] * chunk_size
        sentences = (
            [] if not chunk_size else [None] * (chunk_size * 8)
        )  # Avg 8 sentences per doc
        entity_occurrences = (
            [] if not chunk_size else [None] * (chunk_size * 4)
        )  # Avg 4 entities per doc
        doc_idx = sent_idx = entity_idx = 0

        for doc_id, doc in chunk.items():
            doc_id = int(doc_id)
            if doc_idx < len(documents):
                documents[doc_idx] = (doc_id, doc["title"], 0, 0, 0)
            else:
                documents.append((doc_id, doc["title"], 0, 0, 0))

            for sent_idx_local, sentence in enumerate(doc["sentences"]):
                if sent_idx < len(sentences):
                    sentences[sent_idx] = (
                        sentence["text"],
                        sent_idx_local,
                        doc_id,
                        sentence["word_count"],
                        sentence["token_count"],
                        sentence["alpha_count"],
                    )
                else:
                    sentences.append(
                        (
                            sentence["text"],
                            sent_idx_local,
                            doc_id,
                            sentence["word_count"],
                            sentence["token_count"],
                            sentence["alpha_count"],
                        )
                    )
                sent_idx += 1

                for entity_type, entities in sentence["entities"].items():
                    entity_id = named_entities[entity_type]
                    for entity_idx_local, entity in enumerate(entities):
                        span_start, span_end = sentence["entity_spans"][entity_type][
                            entity_idx_local
                        ]
                        occurrence = (
                            None,
                            entity,
                            span_start,
                            span_end,
                            None,
                            entity_id,
                            doc_id,
                            sent_idx_local,
                            None,
                        )
                        if entity_idx < len(entity_occurrences):
                            entity_occurrences[entity_idx] = occurrence
                        else:
                            entity_occurrences.append(occurrence)
                        entity_idx += 1
            doc_idx += 1

        # Trim any unused pre-allocated space
        if doc_idx < len(documents):
            documents = documents[:doc_idx]
        if sent_idx < len(sentences):
            sentences = sentences[:sent_idx]
        if entity_idx < len(entity_occurrences):
            entity_occurrences = entity_occurrences[:entity_idx]

        return documents, sentences, named_entities, entity_occurrences
    finally:
        # Clear references to help with garbage collection
        chunk.clear()
        gc.collect()


def batch_insert(
    cursor: sqlite3.Cursor,
    table: str,
    rows: List[Tuple],
    batch_size: int = 7000000,
    logger=None,
):
    """Insert rows in batches to avoid memory issues"""
    try:
        total_rows = len(rows)
        start_time = time.time()
        start_memory = get_memory_usage()

        if total_rows == 0:
            return

        # Prepare the insert statement once
        if table == "documents":
            stmt = "INSERT INTO documents VALUES (?, ?, ?, ?, ?)"
        elif table == "sentences":
            stmt = "INSERT INTO sentences VALUES (?, ?, ?, ?, ?, ?)"
        elif table == "named_entities":
            stmt = "INSERT OR IGNORE INTO named_entities VALUES (?, ?, ?)"
        elif table == "entity_occurrences":
            stmt = "INSERT INTO entity_occurrences VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"

        cursor.execute("SAVEPOINT batch_insert")

        for i in range(0, total_rows, batch_size):
            batch_start_time = time.time()
            batch_start_memory = get_memory_usage()

            batch = rows[i : i + batch_size]
            cursor.executemany(stmt, batch)

            # Release memory from the processed batch
            batch.clear()

            batch_end_time = time.time()
            batch_end_memory = get_memory_usage()

            if logger:
                logger.debug(
                    f"{table} batch {i//batch_size + 1}: {len(batch)} rows, "
                    f"Time: {batch_end_time - batch_start_time:.2f}s, "
                    f"Memory: {batch_end_memory - batch_start_memory:.2f}MB"
                )

            # Collect garbage after each batch
            gc.collect()

        cursor.execute("RELEASE batch_insert")

        end_time = time.time()
        end_memory = get_memory_usage()

        if logger:
            logger.debug(
                f"{table} total: {total_rows} rows, "
                f"Total Time: {end_time - start_time:.2f}s, "
                f"Total Memory: {end_memory - start_memory:.2f}MB"
            )
    finally:
        # Clear the main list after processing
        rows.clear()
        gc.collect()


def insert_data(
    db_path: str, data: Tuple, batch_size: int = 200000, logger=None
):
    # Enable URI connection string but without exclusive locking
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        try:
            start_time = time.time()
            start_memory = get_memory_usage()

            # Set optimal performance pragmas without exclusive locking
            pragmas = """
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            PRAGMA cache_size=-2000000;
            PRAGMA temp_store=MEMORY;
            PRAGMA page_size=4096;
            PRAGMA mmap_size=30000000000;
            PRAGMA threads=4;
            PRAGMA busy_timeout=60000;
            PRAGMA count_changes=OFF;
            PRAGMA wal_autocheckpoint=1000;
            """
            cursor.executescript(pragmas)

            documents, sentences, named_entities, entity_occurrences = data

            # Start transaction
            cursor.execute("BEGIN TRANSACTION")

            # Insert documents first
            batch_insert(cursor, "documents", documents, batch_size, logger)
            documents.clear()

            # Then sentences
            batch_insert(cursor, "sentences", sentences, batch_size, logger)
            sentences.clear()

            # Then named entities
            named_entities_rows = [(v, k, None) for k, v in named_entities.items()]
            batch_insert(cursor, "named_entities", named_entities_rows, batch_size, logger)
            named_entities.clear()

            # Finally entity occurrences
            batch_insert(
                cursor, "entity_occurrences", entity_occurrences, batch_size, logger
            )
            entity_occurrences.clear()

            conn.commit()

            end_time = time.time()
            end_memory = get_memory_usage()

            if logger:
                logger.info(
                    f"Total insertion completed - "
                    f"Time: {end_time - start_time:.2f}s, "
                    f"Memory: {end_memory - start_memory:.2f}MB"
                )
        except Exception as e:
            error_msg = f"Error during batch insert: {e}"
            if logger:
                logger.error(error_msg)
            print(error_msg)
            conn.rollback()
            raise
        finally:
            gc.collect()


def json_to_sqlite(
    json_path: str,
    db: EasyNerDBHandler,
    chunk_size: int = 100000,
    batch_size: int = 10000000,
    file_span: Tuple[int, int] = None,
):
    """Process JSON files in chunks and insert data in batches

    Args:
        json_path: Path to JSON file or directory containing JSON files
        db_path: Path to SQLite database
        chunk_size: Number of documents to process at once from JSON (default: 10000)
        batch_size: Number of rows to insert in a single batch (default: 200000)
    """
    logger = db.logger

    logger.info(f"Starting JSON to SQLite conversion")
    logger.info(f"Input path: {json_path}")
    logger.info(f"Database path: {db.db_path}")
    logger.info(f"Chunk size: {chunk_size}")
    logger.info(f"Batch size: {batch_size}")

    if os.path.isdir(json_path):
        json_files = (
            sorted(glob(os.path.join(json_path, "*.json")))[file_span[0] : file_span[1]]
            if file_span
            else glob(os.path.join(json_path, "*.json"))
        )


        # Check for already processed files
        processed_files = db.execute("SELECT file_name FROM source_files")
        processed_files = [file[0] for file in processed_files]
        json_files_new = [
            file for file in json_files if os.path.basename(file) not in processed_files
        ]
        logger.info(
            f"Found {len(json_files)} JSON files to process of which {len(json_files_new)} are not already processed"
        )
    else:
        raise ValueError("Invalid path. Please provide a valid directory path to JSON files.")

    start_time = time.time()
    start_memory = get_memory_usage()

    try:
        for json_file in tqdm(json_files_new, desc="Processing JSON files"):
            try:
                file_start_time = time.time()
                file_start_memory = get_memory_usage()

                file_size = os.path.getsize(json_file) / 1024 / 1024
                logger.info(f"Processing file: {json_file}, 'file_size': {file_size}")
                with open(json_file, "r") as f:
                    data = json.load(f)

                total_items = len(data)
                logger.info(f"File contains {total_items} items")

                for i in range(0, total_items, chunk_size):
                    chunk = dict(list(data.items())[i : i + chunk_size])
                    processed_data = process_chunk(chunk)
                    insert_data(db.db_path, data=processed_data, batch_size=batch_size, logger=logger)
                    del chunk
                    del processed_data
                    gc.collect()

                data.clear()
                gc.collect()

                file_end_time = time.time()
                file_end_memory = get_memory_usage()

                logger.info(
                    f"Completed processing {json_file} - "
                    f"Time: {file_end_time - file_start_time:.2f}s, "
                    f"Memory: {file_end_memory - file_start_memory:.2f}MB"
                )
            except KeyboardInterrupt:
                logger.error("Keyboard interrupt detected, stopping processing")
                raise  # re-raise to exit out of the loop
            except Exception as e:
                logger.error(f"Error processing file {json_file}: {e}")
                continue
            finally:
                # Record fully prodcessed files in TABLE source_files
                try:
                    db.execute(
                        "INSERT INTO source_files (file_name, file_size, import_date) VALUES (?, ?, datetime('now'))",
                        (os.path.basename(json_file), file_size),
                    )
                    db.commit()
                except Exception as e:
                    logger.error(f"Error recording processed file: {e}")
                    continue
                finally:
                    logger.debug(f"Recorded processed file: {json_file}")

    except KeyboardInterrupt:
        logger.info(
            "Processing halted by user. Rolling back any open transactions if needed."
        )
    finally:
        db.close()
        end_time = time.time()
        end_memory = get_memory_usage()
        logger.info(
            f"All processing completed - "
            f"Total Time: {end_time - start_time:.2f}s, "
            f"Final Memory Usage: {end_memory:.2f}MB, "
            f"Memory Change: {end_memory - start_memory:.2f}MB"
        )


if __name__ == "__main__":
    db = EasyNerDBHandler()
    if "import_data_path" not in db.config:
        raise ValueError("Database does not 'data_path' key.")
    data_dir = db.config["import_data_path"]
    json_to_sqlite(data_dir, db, file_span=os.environ.get("FILE_SPAN", None))
    db = EasyNerDBHandler()
    db.data_exchanger.rename_named_entity("phenomenon", "PNM")
    db.data_exchanger.rename_named_entity("disease", "DIS")
    db.data_cleaner.clean_inclusive_entity_spans("PNM")
    db.analysis.doc_calc_word_counts()
    db.data_cleaner.set_error_entity_error_codes()
