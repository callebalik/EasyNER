import json
import sqlite3
import os
from glob import glob
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
from threading import Thread, Event
import time
import logging

# Configure logging with more detailed format and force flush
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    filename='process.log',
    filemode='w',
    force=True
)

# Add a console handler to see logs in real-time
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger().addHandler(console_handler)


def insert_data(conn, data, json_file, progress_bar=None):
    """
    Insert data from a JSON file into the SQLite database.
    """
    cursor = conn.cursor()
    batch_size = 100  # Process sentences in batches of 100
    try:
        # Set pragmas for better write performance
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA journal_mode = MEMORY")
        cursor.execute("PRAGMA cache_size = 100000")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("PRAGMA count_changes = OFF")
        cursor.execute("PRAGMA page_size = 4096")
        
        cursor.execute("BEGIN TRANSACTION;")

        inserted = 0
        for doc_id, document in data.items():
            try:
                title = document["title"]
                logging.debug(f"Processing document {doc_id}: {title[:50]}...")
                
                # Track entity statistics
                entity_stats = {}
                total_entities = 0
                
                cursor.execute(
                    "INSERT OR IGNORE INTO documents (id, title) VALUES (?, ?)",
                    (doc_id, title),
                )

                # Process sentences in batches
                sentences = []
                total_sentences = len(document['sentences'])
                
                for i in range(0, total_sentences, batch_size):
                    batch_sentences = []
                    batch_end = min(i + batch_size, total_sentences)
                    
                    for sentence_index in range(i, batch_end):
                        sentence = document['sentences'][sentence_index]
                        text = sentence["text"]
                        word_count = sentence.get("word_count", 0)
                        token_count = sentence.get("token_count", 0)
                        alpha_count = sentence.get("alpha_count", 0)

                        batch_sentences.append(
                            (text, sentence_index, doc_id, word_count, token_count, alpha_count)
                        )

                    if batch_sentences:
                        cursor.executemany(
                            """
                            INSERT INTO sentences (text, sentence_index, document_id, word_count, token_count, alpha_count)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """,
                            batch_sentences,
                        )

                # Process entity occurrences in batches
                entity_occurrences = []
                
                for sentence_index, sentence in enumerate(document["sentences"]):
                    if not sentence.get("entities"):
                        continue  # Skip if no entities in this sentence
                        
                    cursor.execute(
                        "SELECT id FROM sentences WHERE document_id = ? AND sentence_index = ?",
                        (doc_id, sentence_index),
                    )
                    result = cursor.fetchone()
                    if not result:
                        logging.error(f"Could not find sentence_id for document {doc_id}, sentence {sentence_index}")
                        continue
                    sentence_id = result[0]
                    
                    for entity, entity_texts in sentence["entities"].items():
                        cursor.execute("SELECT id FROM named_entities WHERE named_entity = ?", (entity,))
                        entity_id = cursor.fetchone()
                        if entity_id is None:
                            cursor.execute(
                                "INSERT INTO named_entities (named_entity) VALUES (?)", (entity,)
                            )
                            entity_id = cursor.lastrowid
                        else:
                            entity_id = entity_id[0]
                        
                        # Update entity statistics
                        entity_stats[entity] = entity_stats.get(entity, 0) + len(entity_texts)
                        total_entities += len(entity_texts)
                        
                        for entity_text, span in zip(
                            entity_texts, sentence["entity_spans"][entity]
                        ):
                            span_start, span_end = span
                            entity_occurrences.append(
                                (
                                    entity_text,
                                    span_start,
                                    span_end,
                                    entity_id,
                                    sentence_id
                                )
                            )
                            
                            # Insert in batches of 1000 entity occurrences
                            if len(entity_occurrences) >= 1000:
                                cursor.executemany(
                                    """
                                    INSERT INTO entity_occurrences (entity_text, span_start, span_end, entity_id, sentence_id)
                                    VALUES (?, ?, ?, ?, ?)
                                """,
                                    entity_occurrences,
                                )
                                entity_occurrences = []  # Clear the batch
                
                # Insert any remaining entity occurrences
                if entity_occurrences:
                    cursor.executemany(
                        """
                        INSERT INTO entity_occurrences (entity_text, span_start, span_end, entity_id, sentence_id)
                        VALUES (?, ?, ?, ?, ?)
                    """,
                        entity_occurrences,
                    )

                # Format entity statistics
                entity_stats_str = ", ".join([f"{k}:{v}" for k, v in entity_stats.items()])
                logging.debug(f"Successfully inserted {total_sentences} sentences with {total_entities} entities: ({entity_stats_str})")

                inserted += 1
                if progress_bar:
                    progress_bar.update(1)  # Update progress for each document inserted

            except KeyError as e:
                logging.error(f"Missing required field in document {doc_id}: {e}")
                continue
            except Exception as e:
                logging.error(f"Error processing document {doc_id}: {e}")
                continue

        cursor.execute(
            "INSERT OR REPLACE INTO source_files (file_name, inserted_at) VALUES (?, datetime('now'))",
            (json_file,),
        )
        conn.commit()
        
        return inserted
        
    except Exception as e:
        logging.error(f"Error in insert_data for {json_file}: {e}")
        conn.rollback()
        return 0
    finally:
        # Reset pragmas to default values
        cursor.execute("PRAGMA synchronous = FULL")
        cursor.execute("PRAGMA journal_mode = DELETE")
        cursor.execute("PRAGMA cache_size = 2000")
        cursor.execute("PRAGMA temp_store = DEFAULT")
        cursor.execute("PRAGMA count_changes = ON")


def process_json_file(json_file, queue, processed_files, stop_event, progress_bar):
    """
    Process a JSON file and add its data to the queue if it has not been processed.
    """
    try:
        if stop_event.is_set():
            return
        if json_file in processed_files:
            return
        with open(json_file, "r", encoding="utf-8") as file:
            data = json.load(file)
            doc_count = len(data)  # Count documents in this file
            queue.put((json_file, data, doc_count))  # Add doc_count to queue item
        progress_bar.update(1)
    except Exception as e:
        logging.error(f"Error processing file {json_file}: {e}")


def db_writer(queue, db_path, stop_event):
    """
    Write data from the queue to the SQLite database.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL;")  # Enable Write-Ahead Logging
    cursor.execute("PRAGMA synchronous=NORMAL;")  # Reduce synchronous mode
    cursor.execute("PRAGMA cache_size=10000;")  # Increase cache size
    
    progress_bar = tqdm(total=0, desc=f"Writing documents to {db_path}")  # Start with 0, will update dynamically
    
    while not stop_event.is_set() or not queue.empty():
        try:
            item = queue.get(timeout=1)
            if item is None:
                break
                
            json_file, data, doc_count = item
            progress_bar.total += doc_count  # Update total with new documents
            progress_bar.refresh()
            
            try:
                insert_data(conn, data, json_file, progress_bar)
            except sqlite3.Error as e:
                logging.error(f"SQLite error while inserting data from {json_file}: {e}")
                conn.rollback()
            except Exception as e:
                logging.error(f"Unexpected error while inserting data from {json_file}: {e}", exc_info=True)
                conn.rollback()
        except Empty:
            continue
        except Exception as e:
            logging.error(f"Error in db_writer queue processing: {e}", exc_info=True)
            continue
            
    if progress_bar:
        progress_bar.close()
    conn.close()


def load_json_to_db(json_files, db_path, max_workers=4, chunk_size=20, queue_size=2):
    """
    Load data from JSON files into the SQLite database using multiple threads.
    
    Args:
        json_files: List of JSON files to process
        db_path: Path to the SQLite database
        max_workers: Number of worker threads for processing JSON files
        chunk_size: Number of files to process in each chunk
        queue_size: Maximum size of the queue. Small queue size ensures writer keeps up with processors.
    """
    queue = Queue(maxsize=queue_size)  # Smaller queue size to prevent memory buildup
    stop_event = Event()
    processed_files = set()

    # Ensure the database and tables are created
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Load the list of processed files into memory
    logging.info("Loading list of processed files...")
    start_time = time.time()
    cursor.execute("SELECT file_name FROM source_files")
    processed_files = set(row[0] for row in cursor.fetchall())
    conn.close()
    logging.info(
        f"Loaded {len(processed_files)} processed files in {time.time() - start_time:.2f} seconds"
    )

    files_to_process = [
        json_file for json_file in json_files if json_file not in processed_files
    ]

    if not files_to_process:
        logging.info("All files are already processed. Exiting...")
        return

    logging.info(f"Total files to process: {len(files_to_process)}")
    writer_thread = Thread(
        target=db_writer, args=(queue, db_path, stop_event)  # Removed total_files parameter
    )
    writer_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            progress_bar = tqdm(
                total=len(files_to_process), desc="Processing JSON files"
            )
            for i in range(0, len(files_to_process), chunk_size):
                chunk = files_to_process[i : i + chunk_size]
                for json_file in chunk:
                    futures.append(
                        executor.submit(
                            process_json_file,
                            json_file,
                            queue,
                            processed_files,
                            stop_event,
                            progress_bar,
                        )
                    )
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
            progress_bar.close()
    except KeyboardInterrupt:
        logging.info("Process interrupted by user. Shutting down...")
        stop_event.set()
        for future in futures:
            future.cancel()
    finally:
        queue.put(None)
        writer_thread.join()


def get_total_json_size(json_files):
    """
    Calculate the total size of all JSON files.
    """
    total_size = sum(os.path.getsize(json_file) for json_file in json_files)
    return total_size


def compare_sizes(json_files, db_path):
    """
    Compare the total size of JSON files with the size of the SQLite database.
    """
    total_json_size = get_total_json_size(json_files)
    db_size = os.path.getsize(db_path)
    logging.info(f"Total size of JSON files: {total_json_size / (1024 * 1024):.2f} MB")
    logging.info(f"Size of the database file: {db_size / (1024 * 1024):.2f} MB")
    compression_ratio = total_json_size / db_size
    logging.info(f"Compression ratio: {compression_ratio:.2f}")


def run(data_files, db_path):
    """
    Run the process of loading JSON data into the SQLite database and comparing sizes.
    """
    try:
        load_json_to_db(data_files, db_path)
        compare_sizes(data_files, db_path)
    except KeyboardInterrupt:
        logging.info("Process interrupted by user. Exiting...")
    finally:
        # Ensure the database connection is closed
        conn = sqlite3.connect(db_path)
        conn.close()


if __name__ == "__main__":
    data_dir = "/lunarc/nobackup/projects/snic2020-6-41/carl"
    input_folder = f"{data_dir}/ner_merged_plurals_and_word_count/"

    data_files = sorted(
        glob(f"{input_folder}*.json"),
        key=lambda f: int("".join(filter(str.isdigit, f))),
    )

    # db_path = f"{data_dir}/pubmed_abstracts2.db"
    script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = os.path.join(script_dir, "database.db")
    run(data_files[5:10], db_path)
