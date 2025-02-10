import json
import sqlite3
import os
import gc
from glob import glob
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from typing import List, Tuple, Dict

def process_chunk(chunk):
    documents = []
    sentences = []
    named_entities = {'disease': 1, 'phenomenon': 2}  # Fixed entity types
    entity_occurrences = []

    try:
        for doc_id, doc in chunk.items():
            doc_id = int(doc_id)
            documents.append((doc_id, doc['title'], 0, 0, 0))
            for sent_idx, sentence in enumerate(doc['sentences']):
                # Modified to match schema: text, sentence_index, document_id, word_count, token_count, alpha_count
                sentences.append((sentence['text'], sent_idx, doc_id, 0, 0, 0))
                for entity_type, entities in sentence['entities'].items():
                    entity_id = named_entities[entity_type]
                    for entity_idx, entity in enumerate(entities):
                        span_start, span_end = sentence['entity_spans'][entity_type][entity_idx]
                        occurrence = (None, entity, span_start, span_end, entity_id, sent_idx, 0, 0.0, 0, 0.0, 0.0)
                        entity_occurrences.append(occurrence)

        return documents, sentences, named_entities, entity_occurrences
    finally:
        # Clear references to help with garbage collection
        chunk.clear()
        gc.collect()

def batch_insert(cursor: sqlite3.Cursor, table: str, rows: List[Tuple], batch_size: int = 50000):
    """Insert rows in batches to avoid memory issues"""
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            if table == 'documents':
                cursor.executemany('INSERT INTO documents VALUES (?, ?, ?, ?, ?)', batch)
            elif table == 'sentences':
                cursor.executemany('INSERT INTO sentences VALUES (?, ?, ?, ?, ?, ?)', batch)
            elif table == 'named_entities':
                cursor.executemany('INSERT OR IGNORE INTO named_entities VALUES (?, ?)', batch)
            elif table == 'entity_occurrences':
                cursor.executemany('INSERT INTO entity_occurrences VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', batch)
            batch.clear()
            gc.collect()
    finally:
        # Clear the main list after processing
        rows.clear()
        gc.collect()

def insert_data(db_path: str, data: Tuple, batch_size: int = 50000):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    documents, sentences, named_entities, entity_occurrences = data
    
    try:
        # Set pragmas for better write performance
        cursor.execute("PRAGMA synchronous = OFF")
        cursor.execute("PRAGMA journal_mode = MEMORY")
        cursor.execute("PRAGMA cache_size = 100000")
        cursor.execute("PRAGMA temp_store = MEMORY")
        cursor.execute("BEGIN TRANSACTION")

        # Insert documents first
        batch_insert(cursor, 'documents', documents)
        documents.clear()
        
        # Then sentences
        batch_insert(cursor, 'sentences', sentences)
        sentences.clear()
        
        # Then named entities
        named_entities_rows = [(v, k) for k, v in named_entities.items()]
        batch_insert(cursor, 'named_entities', named_entities_rows)
        named_entities.clear()
        
        # Finally entity occurrences
        batch_insert(cursor, 'entity_occurrences', entity_occurrences)
        entity_occurrences.clear()
        
        conn.commit()
    except Exception as e:
        print(f"Error during batch insert: {e}")
        conn.rollback()
        raise
    finally:
        # Reset pragmas to default values
        cursor.execute("PRAGMA synchronous = FULL")
        cursor.execute("PRAGMA journal_mode = DELETE")
        cursor.execute("PRAGMA cache_size = 2000")
        cursor.execute("PRAGMA temp_store = DEFAULT")
        conn.close()
        gc.collect()

def json_to_sqlite(json_path: str, db_path: str, chunk_size: int = 1000, batch_size: int = 50000):
    """Process JSON files in chunks and insert data in batches"""
    if os.path.isdir(json_path):
        json_files = glob(os.path.join(json_path, "*.json"))
    else:
        json_files = [json_path]

    for json_file in tqdm(json_files, desc="Processing JSON files"):
        try:
            with open(json_file, 'r') as f:
                data = json.load(f)

            # Process data in chunks to avoid memory issues
            total_items = len(data)
            for i in range(0, total_items, chunk_size):
                chunk = dict(list(data.items())[i:i + chunk_size])
                processed_data = process_chunk(chunk)
                insert_data(db_path, processed_data, batch_size)
                del chunk
                del processed_data
                gc.collect()

            # Clear the data dictionary after processing each file
            data.clear()
            gc.collect()
        except Exception as e:
            print(f"Error processing file {json_file}: {e}")
            continue

if __name__ == '__main__':
    data_dir = "/lunarc/nobackup/projects/snic2020-6-41/carl/ner_merged_plurals_and_word_count"
    db_path = '/home/carloa/Desktop/EasyNer/scripts/database/database.db'
    json_to_sqlite(data_dir, db_path)
