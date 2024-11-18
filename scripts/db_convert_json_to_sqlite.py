import json
import sqlite3
import os
from glob import glob
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from threading import Thread, Event
import time

def create_database(db_path):
    """
    Create the SQLite database and tables if they do not exist.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('PRAGMA journal_mode=WAL;')
    # Create tables
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS articles (
            pmid INTEGER PRIMARY KEY,
            title TEXT
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sentences (
            id INTEGER PRIMARY KEY,
            text TEXT,
            sentence_index INTEGER,
            pmid INTEGER,
            FOREIGN KEY(pmid) REFERENCES articles(pmid)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS entities (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity TEXT UNIQUE
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS entity_occurrences (
            id INTEGER PRIMARY KEY,
            entity_text TEXT,
            entity_id INTEGER,
            sentence_id INTEGER,
            sentence_index INTEGER,
            span TEXT,
            FOREIGN KEY(sentence_id) REFERENCES sentences(id),
            FOREIGN KEY(entity_id) REFERENCES entities(id)
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS processed_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE
        )
    ''')
    # Create indexes to speed up queries
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_sentences_pmid_sentence_index ON sentences (pmid, sentence_index);')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_entities_entity ON entities (entity);')
    conn.commit()
    return conn

def insert_data(conn, data, filename):
    """
    Insert data from a JSON file into the SQLite database.
    """
    cursor = conn.cursor()
    cursor.execute('BEGIN TRANSACTION;')

    for pmid, article in data.items():
        title = article['title']
        cursor.execute('''
            INSERT OR IGNORE INTO articles (pmid, title)
            VALUES (?, ?)
        ''', (pmid, title))

        sentences = []
        for sentence_index, sentence in enumerate(article['sentences']):
            text = sentence['text']
            sentences.append((pmid, sentence_index, text))

        cursor.executemany('''
            INSERT INTO sentences (pmid, sentence_index, text)
            VALUES (?, ?, ?)
        ''', sentences)

        entities = set()
        entity_occurrences = []
        for sentence_index, sentence in enumerate(article['sentences']):
            cursor.execute('SELECT id FROM sentences WHERE pmid = ? AND sentence_index = ?', (pmid, sentence_index))
            sentence_id = cursor.fetchone()[0]
            for entity, entity_texts in sentence['entities'].items():
                entities.add((entity,))
                cursor.execute('SELECT id FROM entities WHERE entity = ?', (entity,))
                entity_id = cursor.fetchone()
                if entity_id is None:
                    cursor.execute('INSERT INTO entities (entity) VALUES (?)', (entity,))
                    entity_id = cursor.lastrowid
                else:
                    entity_id = entity_id[0]
                for entity_text, span in zip(entity_texts, sentence['entity_spans'][entity]):
                    entity_occurrences.append((sentence_id, sentence_index, entity_id, entity_text, json.dumps(span)))

        cursor.executemany('INSERT OR IGNORE INTO entities (entity) VALUES (?)', entities)
        cursor.executemany('''
            INSERT INTO entity_occurrences (sentence_id, sentence_index, entity_id, entity_text, span)
            VALUES (?, ?, ?, ?, ?)
        ''', entity_occurrences)

    cursor.execute('INSERT INTO processed_files (filename) VALUES (?)', (filename,))
    conn.commit()

def process_json_file(json_file, queue, processed_files, stop_event, progress_bar):
    """
    Process a JSON file and add its data to the queue if it has not been processed.
    """
    try:
        if stop_event.is_set():
            return
        if json_file in processed_files:
            print(f"File {json_file} already processed. Skipping.")
            return
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
        queue.put((json_file, data))
        progress_bar.update(1)
    except Exception as e:
        print(f"Error processing file {json_file}: {e}")

def db_writer(queue, db_path, stop_event, total_files):
    """
    Write data from the queue to the SQLite database.
    """
    conn = create_database(db_path)
    cursor = conn.cursor()
    progress_bar = tqdm(total=total_files, desc=f"Writing to {db_path}" )
    while not stop_event.is_set() or not queue.empty():
        try:
            item = queue.get(timeout=1)
            if item is None:
                break
            json_file, data = item
            insert_data(conn, data, json_file)
            progress_bar.update(1)
        except:
            continue
    progress_bar.close()
    conn.close()

def load_json_to_db(json_files, db_path, max_workers=4, chunk_size=20, queue_size=100):
    """
    Load data from JSON files into the SQLite database using multiple threads.
    """
    queue = Queue(maxsize=queue_size)
    stop_event = Event()
    processed_files = set()

    # Ensure the database and tables are created
    conn = create_database(db_path)
    cursor = conn.cursor()

    # Load the list of processed files into memory
    print("Loading list of processed files...")
    start_time = time.time()
    cursor.execute('SELECT filename FROM processed_files')
    processed_files = set(row[0] for row in cursor.fetchall())
    conn.close()
    print(f"Loaded {len(processed_files)} processed files in {time.time() - start_time:.2f} seconds")

    files_to_process = [json_file for json_file in json_files if json_file not in processed_files]

    if not files_to_process:
        print("All files are already processed. Exiting...")
        return

    print(f"Total files to process: {len(files_to_process)}")
    writer_thread = Thread(target=db_writer, args=(queue, db_path, stop_event, len(files_to_process)))
    writer_thread.start()

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            progress_bar = tqdm(total=len(files_to_process), desc="Processing JSON files")
            for i in range(0, len(files_to_process), chunk_size):
                chunk = files_to_process[i:i + chunk_size]
                for json_file in chunk:
                    futures.append(executor.submit(process_json_file, json_file, queue, processed_files, stop_event, progress_bar))
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
            progress_bar.close()
    except KeyboardInterrupt:
        print("Process interrupted by user. Shutting down...")
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
    print(f"Total size of JSON files: {total_json_size / (1024 * 1024):.2f} MB")
    print(f"Size of the database file: {db_size / (1024 * 1024):.2f} MB")
    compression_ratio = total_json_size / db_size
    print(f"Compression ratio: {compression_ratio:.2f}")

def run(data_files, db_path):
    """
    Run the process of loading JSON data into the SQLite database and comparing sizes.
    """
    try:
        load_json_to_db(data_files, db_path)
        compare_sizes(data_files, db_path)
    except KeyboardInterrupt:
        print("Process interrupted by user. Exiting...")
    finally:
        # Ensure the database connection is closed
        conn = sqlite3.connect(db_path)
        conn.close()

if __name__ == "__main__":
    input_folder = "/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/ner_merged/"

    data_files = sorted(
        glob(f"{input_folder}*.json"),
        key=lambda f: int("".join(filter(str.isdigit, f))),
    )

    db_path = '/proj/berzelius-2021-21/users/x_caoll/EasyNer_ner_output/database6.db'
    # data_files = data_files[0:30]
    run(data_files, db_path)